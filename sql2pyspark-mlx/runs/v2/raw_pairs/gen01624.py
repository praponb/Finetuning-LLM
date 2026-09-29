from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("duration_sec").between(268, 465))
    .withColumn("page_filled", F.coalesce("page", F.lit("Unknown")))
)

summary = (
    filtered_web_events
    .groupBy("event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    summary
    .select("event_type", "duration_sec_range")
    .orderBy(F.col("duration_sec_range").desc())
)
