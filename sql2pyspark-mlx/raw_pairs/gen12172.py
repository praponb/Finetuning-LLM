from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("quantity") >= 4)
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
)

order_items_stats = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.min("unit_price_rounded").alias("min_unit_price_rounded"))
)

result = (
    order_items_stats
    .filter(F.col("min_unit_price_rounded") > 46)
    .select("order_id", "min_unit_price_rounded")
    .orderBy(F.col("order_id").desc(), F.col("min_unit_price_rounded").desc())
)
