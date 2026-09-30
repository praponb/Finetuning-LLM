from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("unit_price") >= 390)

order_items_stats = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    order_items_stats
    .filter(F.col("max_unit_price") < 100)
    .select("order_id", "max_unit_price")
    .orderBy(F.col("order_id").desc(), F.col("max_unit_price").desc())
    .limit(10)
)
