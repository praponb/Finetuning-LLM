from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 24) & (F.col("duration_sec") <= 197))
    .groupBy(
        F.when(F.col("duration_sec") >= 367, "high").when(F.col("duration_sec") >= 218, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy(F.col("pct_page_view").desc())
)
