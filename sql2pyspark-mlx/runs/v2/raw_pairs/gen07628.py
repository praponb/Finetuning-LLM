from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("event_type", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") >= 326)
        .select("event_type", "session_id")
    )
    .orderBy("session_id")
)
