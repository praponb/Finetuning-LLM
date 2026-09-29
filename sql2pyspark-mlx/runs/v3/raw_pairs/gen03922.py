from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter((F.col("c.age") == 53) | F.col("o.amount").between(1420, 1500))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)
