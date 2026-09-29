from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 19) & (F.col("event_time") < "2023-08-01") & (F.col("duration_sec") > 281))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type") == "search", 1).otherwise(0).alias("is_search"),
    )
    .orderBy(F.col("is_search").desc(), "id")
    .limit(50)
)
