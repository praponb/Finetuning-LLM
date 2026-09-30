from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("web_events.page"),
        F.col("sessions.country"),
        F.col("sessions.browser"),
    )
    .orderBy("page")
)
