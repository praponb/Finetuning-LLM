from pyspark.sql import functions as F

base = df_web_events.filter(F.col("page") != "/product")

summary = (
    base
    .groupBy(
        F.when(F.col("duration_sec") >= 397, "high").when(F.col("duration_sec") >= 215, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    summary
    .orderBy("duration_sec_bucket", F.col("pct_search").desc())
    .limit(3)
)
