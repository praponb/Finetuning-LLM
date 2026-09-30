from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .withColumn("quantity_whole", F.ceil("quantity"))
)

order_items_stats = (
    src
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = order_items_stats.orderBy(F.col("order_id").desc())
