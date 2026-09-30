from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.order_id").isin([8, 28, 32, 36]))
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 30, "high").when(F.col("o.shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
)
