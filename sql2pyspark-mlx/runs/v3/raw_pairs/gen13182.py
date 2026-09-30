from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "right")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(F.col("web_events.session_id") == 30)
    .groupBy(
        F.when(F.col("sessions.country").isin(["UK", "US"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.count(F.col("web_events.duration_sec")).alias("duration_sec_count"))
)
