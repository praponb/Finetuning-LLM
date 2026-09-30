from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-09-01") & (F.col("event_time") >= "2023-11-05"))
    .groupBy(
        F.when(F.col("page").isin(["/search", "/home"]), "core").otherwise("other").alias("page_group"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
)
