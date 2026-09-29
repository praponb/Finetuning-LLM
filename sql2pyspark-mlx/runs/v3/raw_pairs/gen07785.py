from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "tags", "stock_qty", F.sort_array("tags").alias("sorted_tags"))
    .orderBy(F.col("stock_qty").desc())
)
