from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec") <= 460)
    .select(
        F.col("s.device"),
        F.col("s.country"),
        F.col("we.page"),
        F.date_format(F.col("s.started_at"), "yyyy-MM-dd").alias("started_at_label"),
    )
)
