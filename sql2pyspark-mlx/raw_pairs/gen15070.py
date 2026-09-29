from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-08-01") | F.col("page").isin(["/product", "/checkout", "/home"]))
    .select(
        "id",
        "session_id",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.when(F.col("page") == "/home", 1).otherwise(0).alias("is_home"),
    )
)
