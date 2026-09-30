from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "page",
        "session_id",
        F.when(F.col("page") == "/product", 1).otherwise(0).alias("is_product"),
        F.year("event_time").alias("event_time_year"),
    )
)
