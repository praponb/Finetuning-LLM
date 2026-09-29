from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/cart", "/product"]))
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.browser"), F.col("ss.event_type_list"))
)
