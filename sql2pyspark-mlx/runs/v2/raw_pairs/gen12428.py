from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(178, 292) & F.col("duration_sec").between(276, 347))
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.month("event_time").alias("event_time_month"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
