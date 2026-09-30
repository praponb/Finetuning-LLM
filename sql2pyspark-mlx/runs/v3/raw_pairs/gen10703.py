from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([20, 24, 27, 39]))
    .select(
        "id",
        "session_id",
        "event_type",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
