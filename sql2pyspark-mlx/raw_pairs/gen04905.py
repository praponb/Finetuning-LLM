from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Electronics", "Grocery", "Home"])
    .agg(F.avg("price").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
