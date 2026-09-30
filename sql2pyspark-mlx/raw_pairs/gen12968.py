from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "price", "stock_qty", "tags", F.size("tags").alias("tags_count"))
)
