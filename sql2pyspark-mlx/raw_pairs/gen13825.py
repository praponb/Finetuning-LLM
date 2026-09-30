from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("order_id").isin([6, 23, 34, 40]))

agg = (
    recent_order_items
    .groupBy("order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    agg
    .select("order_id", "stddev_unit_price")
    .orderBy(F.col("order_id").desc(), F.col("stddev_unit_price").desc())
    .limit(20)
)
