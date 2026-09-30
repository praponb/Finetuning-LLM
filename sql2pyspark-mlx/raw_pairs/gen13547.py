from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("sessions.country").isin(["UK", "JP"]))
    .groupBy(
        F.col("customers.segment"),
        F.when(F.col("customers.country").isin(["TH", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.count(F.col("web_events.duration_sec")).alias("duration_sec_count"),
        F.min(F.col("web_events.duration_sec")).alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") > 430)
)
