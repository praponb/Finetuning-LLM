from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("session_id") == 35)
    .withColumn("page_event_type", F.concat("page", F.lit(" - "), "event_type"))
)

web_events_stats = (
    base
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
)

result = web_events_stats.filter(F.col("home_duration_sec") >= 40)
