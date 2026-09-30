from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & F.col("unit_price").between(190, 310))
)

agg = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    agg
    .orderBy("min_unit_price", "order_id")
    .limit(50)
)
