from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/home", "/search"]))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.total_duration_sec") > 1000)
    .select(F.col("s.id"), F.col("s.device"), F.col("t.total_duration_sec"))
    .orderBy("id")
)
