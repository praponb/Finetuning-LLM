from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("order_id").isNotNull()).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .filter(F.col("o.shipping_fee") <= 26)
    .select(F.col("o.id"), F.col("o.store_id"))
)
