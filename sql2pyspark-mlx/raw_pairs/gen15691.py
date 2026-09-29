from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([16, 32, 36]))
    .select(
        "id",
        "session_id",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)
