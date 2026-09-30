from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec") <= 402)
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(F.col("s.country"), F.col("st.event_type_list"))
)
