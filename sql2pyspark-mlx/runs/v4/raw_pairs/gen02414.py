from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "add_to_cart"]) & (F.col("session_id").isin([23, 39]) | (F.col("event_time") >= "2024-10-25")))
    .select(
        "id",
        "page",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)
