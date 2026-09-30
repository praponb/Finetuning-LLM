from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("s.status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
    )
)
