from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.hour("event_time").between(6, 10))
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("st.event_type_list"),
        F.col("st.duration_sec_range"),
    )
)
