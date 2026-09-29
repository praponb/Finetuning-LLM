from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-12 04:00:00").cast("timestamp"))
    .withColumn("adjusted_duration_sec", F.round((F.col("duration_sec") * 1.07), 2))
    .withColumn("event_time_period", F.date_trunc("year", "event_time"))
)

result = (
    cleaned
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/home"])
    .agg(F.sum("adjusted_duration_sec").alias("amount"))
    .select(
        "event_type",
        F.col("/checkout").alias("checkout"),
        F.col("/home").alias("home"),
    )
)
