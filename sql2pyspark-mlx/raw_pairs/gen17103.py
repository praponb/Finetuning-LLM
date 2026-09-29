from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 510, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_total_duration_sec"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
    )
)
