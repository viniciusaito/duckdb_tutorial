# %%
import pandas as pd
import glob
import time
import duckdb

# %%
conn = duckdb.connect() # create an in-memory database

# %%
# Doing stuff with pandas
cur_time = time.time()
# Concatenate everything in pandas
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))

# %%
# Now doing stuff with duckdb
cur_time = time.time()
df = conn.execute("""
    SELECT *
    FROM read_csv_auto('dataset/*.csv', header=True)
    LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)

# %%
df = conn.execute("""
    SELECT *
    FROM read_csv_auto('dataset/*.csv', header=True)
""").df()
conn.register('df_view', df)
conn.execute('DESCRIBE df_view').df()

# %%
conn.execute("""SELECT COUNT(*) FROM df_view""").df()

# %%
df.isnull().sum()
df = df.dropna(how='all')

# %%
# We can read pandas dataframes without issue using DuckDB:
conn.execute("""SELECT COUNT(*) FROM df""").df()

# %%
# Where clause:
conn.execute("""SELECT * FROM df WHERE "Order ID"='295665'""").df()

# %%
conn.execute("""
    CREATE TABLE df_test AS SELECT * FROM df;
    UPDATE df_test
    SET Price = regexp_replace(Price,'[^a-zA-Z0-9\s\.]', '')
    WHERE Price IS NOT NULL;
""")

# %%
conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		"Price"::DECIMAL AS price_each,
		strptime("Order Date", '%Y-%m-%d %H:%M:%S')::DATE as order_date,
		"Purchase Address" AS purchase_address
	FROM df_test
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")

# %%
conn.execute("FROM sales").df()

# %%
conn.execute("""
	SELECT 
		* EXCLUDE (product, order_date, purchase_address)
	FROM sales
	""").df()

# %%
conn.execute("""
	SELECT 
		MIN(COLUMNS(* EXCLUDE (product, order_date, purchase_address))) 
	FROM sales
	""").df()

# %%
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		MONTH(order_date) as month,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price_each) AS revenue
	FROM sales
	GROUP BY ALL
""")

# %%
conn.execute("""
    FROM aggregated_sales
""").df()

# %%
conn.execute("""
    SELECT
        city,
        SUM(revenue) as total
    FROM aggregated_sales
    GROUP BY city
    ORDER BY total DESC
""").df()

# %%
conn.execute("COPY (FROM aggregated_sales) TO 'aggregated_sales.parquet' (FORMAT 'parquet')")

# %%
conn.execute("FROM aggregated_sales.parquet").df()
# %%
