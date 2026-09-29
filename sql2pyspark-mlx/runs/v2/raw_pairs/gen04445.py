from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
    .orderBy("event_time", F.col("id").desc())
    .limit(50)
)
