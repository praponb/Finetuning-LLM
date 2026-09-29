from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("discount") <= 0.24).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.quantity") < 5)
    .select(F.col("oi.id"), F.col("oi.unit_price"))
)
