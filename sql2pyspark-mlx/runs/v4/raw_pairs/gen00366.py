from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(175, 317))
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"))
    .orderBy(F.col("event_month").desc(), F.col("home_count").desc())
)
