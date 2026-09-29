from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").between(221, 281))
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.device"), F.col("t.page_values"))
    .orderBy("device")
)
