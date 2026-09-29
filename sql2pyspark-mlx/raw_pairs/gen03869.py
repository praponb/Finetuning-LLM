from pyspark.sql import functions as F

src = (
    df_web_events
    .filter((F.col("page") == "/product") & (F.col("event_time") >= "2024-03-30"))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

summary = (
    src
    .groupBy(F.month("event_time_day").alias("event_day_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.min("event_time_day").alias("first_event_time_day"),
    )
)

result = (
    summary
    .filter(F.col("home_duration_sec") > 45)
    .select("event_day_month", "first_event_time_day")
    .orderBy("first_event_time_day")
)
