from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 8, 15]) & (F.col("page") == "/cart"))
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)
