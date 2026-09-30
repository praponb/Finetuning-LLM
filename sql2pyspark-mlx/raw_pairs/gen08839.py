from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/cart", "/home"]) & F.col("page").isNull() & (F.col("page") == "/product"))
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
