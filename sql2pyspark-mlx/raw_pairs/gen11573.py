from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 149) & F.col("event_type").isin(["search", "click"]))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy("total_duration_sec")
)
