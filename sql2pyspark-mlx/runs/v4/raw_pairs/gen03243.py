from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "click", "search"]))
    .select(
        "id",
        "page",
        "duration_sec",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
    )
    .orderBy("page_event_type_key", F.col("id").desc())
)
