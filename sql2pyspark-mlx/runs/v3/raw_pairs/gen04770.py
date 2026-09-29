from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.trim("page").alias("clean_page"),
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
    )
)
