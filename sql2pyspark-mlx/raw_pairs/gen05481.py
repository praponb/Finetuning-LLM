from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.discount") > 0.16) & F.col("oi.unit_price").isNotNull())
    .select(F.col("oi.id"), F.col("oi.unit_price"), F.col("o.amount"))
)
