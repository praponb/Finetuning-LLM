from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity") <= 4)

agg = (
    filtered_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    agg
    .select("order_id", "avg_unit_price")
    .orderBy("avg_unit_price", "order_id")
    .limit(20)
)
