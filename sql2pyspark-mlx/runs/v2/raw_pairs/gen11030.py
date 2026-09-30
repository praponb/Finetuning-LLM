from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.country"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.max(F.col("s.started_at")).alias("latest_started_at"))
    .orderBy(F.col("signup_month").desc(), "country")
    .limit(10)
)
