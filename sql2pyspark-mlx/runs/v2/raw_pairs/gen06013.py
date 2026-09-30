from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("order_id").desc(), F.col("customer_id").desc(), "id")
    .limit(10)
)
