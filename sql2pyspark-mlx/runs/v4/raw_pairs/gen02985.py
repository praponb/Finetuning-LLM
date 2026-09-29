from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-04-27 08:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

result = (
    cleaned
    .groupBy("session_id")
    .pivot("page", ["/home", "/search"])
    .agg(F.max("duration_sec").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "session_id",
        F.col("/home_total").alias("home_total"),
        F.col("/home_n").alias("home_n"),
        F.col("/search_total").alias("search_total"),
        F.col("/search_n").alias("search_n"),
    )
)
