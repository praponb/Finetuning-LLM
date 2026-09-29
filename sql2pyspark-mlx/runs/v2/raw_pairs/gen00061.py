from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") >= 33)
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
    .orderBy(F.col("quantity").desc(), "id")
    .limit(3)
)
