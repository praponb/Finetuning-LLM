from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "session_id",
        F.when(F.col("duration_sec") >= 434, "high").when(F.col("duration_sec") >= 82, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
