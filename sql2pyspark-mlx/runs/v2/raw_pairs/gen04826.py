from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
    )
    .orderBy("start_date")
)
