from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click", "purchase"]))
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy(F.col("row_count").desc(), "page", "event_month")
    .limit(20)
)
