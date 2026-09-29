from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("customers.is_active"))
    .groupBy(
        F.when(F.col("customers.credit_limit") >= 15700, "high").when(F.col("customers.credit_limit") >= 3400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
    )
    .orderBy("credit_limit_bucket")
    .limit(5)
)
