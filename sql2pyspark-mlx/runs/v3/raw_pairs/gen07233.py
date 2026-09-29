from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") < 42)
    .select("order_id", "avg_unit_price")
    .orderBy(F.col("avg_unit_price").desc())
)
