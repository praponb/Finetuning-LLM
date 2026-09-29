from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 4)
    .groupBy(
        F.when(F.col("duration_sec") >= 404, "high").when(F.col("duration_sec") >= 136, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") >= 1100)
    .select("duration_sec_bucket", "pct_home", "total_duration_sec")
    .orderBy(F.col("total_duration_sec").desc())
)
