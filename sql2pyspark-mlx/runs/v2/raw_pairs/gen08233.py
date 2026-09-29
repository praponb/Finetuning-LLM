from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "search"]))
    .select(
        "event_type",
        "session_id",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)
