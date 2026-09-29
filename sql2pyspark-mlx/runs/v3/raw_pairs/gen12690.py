from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 35)
    .groupBy(
        F.when(F.col("duration_sec") >= 406, "high").when(F.col("duration_sec") >= 211, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") > 440)
    .orderBy("min_duration_sec")
)
