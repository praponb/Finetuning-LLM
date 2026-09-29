from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@startup.io"))
    .select(
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
)
