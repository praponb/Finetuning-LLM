from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") >= 320)
    .orderBy(F.col("avg_unit_price").desc(), "order_id")
    .limit(10)
)
