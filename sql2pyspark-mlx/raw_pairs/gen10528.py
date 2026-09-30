from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("web_events.page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)
