from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("o.store_id").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
    )
    .orderBy("id")
)
