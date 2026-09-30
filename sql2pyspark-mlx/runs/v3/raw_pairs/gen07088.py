from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("quantity").desc())
)
