from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-10-01") | (F.col("duration_sec") != 134))
    .select(
        "id",
        "page",
        "event_time",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
    .orderBy(F.col("page").desc())
)
