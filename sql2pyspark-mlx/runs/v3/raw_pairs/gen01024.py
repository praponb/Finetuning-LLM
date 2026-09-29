from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec") > 158)
    .groupBy(F.year(F.col("s.started_at")).alias("started_year"))
    .agg(F.count(F.when(F.col("we.event_type") == "click", 1)).alias("click_count"))
    .orderBy(F.col("click_count").desc(), "started_year")
    .limit(3)
)
