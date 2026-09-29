from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 10) & (F.col("revenue") <= 3800))
    .select("id", "revenue", F.col("units").cast("string").alias("units_str"))
)
