from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select(
        F.col("s.status"),
        F.col("s.start_date"),
        F.col("c.email"),
        F.col("c.country"),
        F.col("c.age"),
    )
)
