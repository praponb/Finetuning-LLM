from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page") != "/cart")
    .select(
        "*",
        F.substring("page", 1, 2).alias("page_prefix"),
        F.concat_ws(",", "event_type", "page").alias("event_type_page_key"),
    )
)

result = (
    cleaned
    .groupBy("session_id")
    .pivot("page", ["/home", "/search"])
    .agg(F.sum("duration_sec"))
    .select(
        "session_id",
        F.col("/home").alias("home"),
        F.col("/search").alias("search"),
    )
)
