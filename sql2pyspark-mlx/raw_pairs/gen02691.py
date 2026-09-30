from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec").between(239, 292) & (F.col("event_time") < "2025-01-03")) | (F.col("event_type") == "page_view"))
    .select(
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
