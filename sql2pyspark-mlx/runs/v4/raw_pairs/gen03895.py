from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("price") >= 1200) & F.col("launch_date").between("2025-06-01", "2025-07-01")) | (F.col("stock_qty") < 265))
)
