from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter((F.col("we.duration_sec") > 272) & (F.col("s.device") == "mobile"))
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("we.page"),
        F.date_format(F.col("we.event_time"), "MMM yyyy").alias("event_time_label"),
    )
)
