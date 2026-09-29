from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.coalesce(F.col("o.shipping_fee"), F.lit(0)).alias("shipping_fee_filled"),
    )
    .orderBy("customer_id")
)
