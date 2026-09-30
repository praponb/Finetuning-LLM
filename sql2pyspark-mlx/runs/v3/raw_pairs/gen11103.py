from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3600) & F.col("product_id").isin([6, 8, 9]) & F.col("units").isNotNull())
    .select("id", "store_id")
)
