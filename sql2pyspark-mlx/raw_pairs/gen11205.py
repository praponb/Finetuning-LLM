from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)
