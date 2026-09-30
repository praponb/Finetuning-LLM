from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([23, 26]) & (F.col("session_id").isin([9, 28, 38]) | F.col("event_type").isin(["page_view", "click"])))
    .select(
        "id",
        "page",
        "event_type",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
