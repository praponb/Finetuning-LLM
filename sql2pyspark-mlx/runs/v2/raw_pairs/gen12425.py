from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 450, "high").when(F.col("duration_sec") >= 90, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)
