from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Books", "Grocery"])
    .agg(F.avg("cost").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
