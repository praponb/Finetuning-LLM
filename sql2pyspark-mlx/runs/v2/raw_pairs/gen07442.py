from pyspark.sql import functions as F

base = df_order_items.filter(F.col("unit_price") < 120)

product_id_summary = (
    base
    .groupBy("product_id", "order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = product_id_summary.orderBy(F.col("order_id").desc())
