from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") <= 320) | F.array_contains("tags", "sale"))
    .select("id", "tags", "brand", "stock_qty", F.size("tags").alias("tags_count"))
)
