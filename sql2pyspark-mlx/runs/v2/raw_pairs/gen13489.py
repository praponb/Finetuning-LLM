from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.end_date") < "2023-09-01")
    .select(
        F.col("s.customer_id"),
        F.col("s.plan"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.substring(F.col("s.status"), 1, 2).alias("status_prefix"),
    )
    .orderBy("plan")
)
