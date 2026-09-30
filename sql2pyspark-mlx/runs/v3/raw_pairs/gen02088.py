from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("oi.product_id").isNotNull() & (F.col("oi.unit_price") > 300))
    .select(
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
)
