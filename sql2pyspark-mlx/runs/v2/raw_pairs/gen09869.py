from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(~((F.col("s.started_at") >= "2023-10-01") | (F.col("we.duration_sec") > 200)))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.concat(F.col("we.event_type"), F.lit(" "), F.col("we.page")).alias("event_type_page"),
    )
    .orderBy(F.col("session_id").desc())
)
