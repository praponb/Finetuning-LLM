from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.amount").isNotNull() & (F.col("c.signup_date") >= "2025-03-20"))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.email"),
    )
    .orderBy("country")
)
