from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.is_active"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
    .orderBy("signup_month", "min_started_at")
)
