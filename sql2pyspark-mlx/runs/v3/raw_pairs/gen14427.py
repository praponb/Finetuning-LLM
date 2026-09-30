from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(~F.col("event_type").isin(["click", "search"])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.started_at"))
    .orderBy("started_at", "id")
)
