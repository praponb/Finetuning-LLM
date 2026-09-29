from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(120, 260))
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") > 49)
    .orderBy(F.col("avg_unit_price").desc())
)
