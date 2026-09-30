from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("session_id").isin([16, 27, 28]) & (F.col("event_type") == "purchase")) | (F.col("duration_sec") != 149))
    .withColumn("event_type_page", F.concat("event_type", F.lit(" - "), "page"))
)

summary = (
    filtered_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("total_duration_sec") < 2300)
    .select("event_year", "total_duration_sec", "avg_duration_sec")
    .orderBy(F.col("avg_duration_sec").desc())
)
