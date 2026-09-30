from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-04-01") | ~F.col("page").isin(["/home", "/checkout"]))
    .select(
        "id",
        "page",
        F.concat_ws(",", "page", "event_type").alias("page_event_type_key"),
    )
)
