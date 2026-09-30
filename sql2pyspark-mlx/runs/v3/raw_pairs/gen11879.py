from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        "duration_sec",
        "session_id",
        F.length("event_type").alias("event_type_length"),
        F.when(F.col("page") == "/search", 1).otherwise(0).alias("is_search"),
    )
)
