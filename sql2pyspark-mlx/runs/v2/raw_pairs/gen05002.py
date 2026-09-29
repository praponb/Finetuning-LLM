from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.browser") == "Safari")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.max(F.col("s.started_at")).alias("last_started_at"))
    .orderBy("last_started_at")
)
