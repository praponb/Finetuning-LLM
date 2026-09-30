from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([17, 27]) & (F.col("page") != "/home"))
    .select(
        "id",
        "event_time",
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
