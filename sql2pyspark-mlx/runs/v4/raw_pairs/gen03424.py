from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("event_time") >= "2025-01-18")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.85), 2).alias("adjusted_duration_sec"),
    )
)

result = (
    valid
    .groupBy("event_type")
    .pivot("page", ["/product", "/home"])
    .agg(F.max("adjusted_duration_sec"))
    .select(
        "event_type",
        F.col("/product").alias("product"),
        F.col("/home").alias("home"),
    )
)
