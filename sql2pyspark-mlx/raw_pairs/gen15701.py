from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 336) & (F.col("event_type") != "purchase"))
    .groupBy(
        F.when(F.col("duration_sec") >= 462, "high").when(F.col("duration_sec") >= 74, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)
