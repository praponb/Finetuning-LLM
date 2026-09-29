from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 446) & F.col("duration_sec").isNull())
    .groupBy(F.to_date("event_time").alias("event_day"), "page")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("page").desc())
)
