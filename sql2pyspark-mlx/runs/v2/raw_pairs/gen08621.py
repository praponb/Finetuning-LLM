from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.lower(F.trim(F.col("c.name"))) == "carol diaz")
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("o.store_id"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
)
