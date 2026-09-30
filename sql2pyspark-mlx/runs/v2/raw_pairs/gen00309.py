from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "click", "purchase"]))
    .select("id", "duration_sec", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("page") == "/cart")
        .select("id", "duration_sec", "session_id")
    )
    .distinct()
)
