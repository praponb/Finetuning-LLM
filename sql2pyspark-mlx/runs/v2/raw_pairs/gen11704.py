from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(216, 336) & ((F.col("event_time") >= "2024-04-01") | F.col("event_type").isin(["purchase", "add_to_cart"])))
    .groupBy("page", "event_type")
    .agg(
        F.max("event_time").alias("last_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") < 21)
    .orderBy("page", "event_type")
    .limit(50)
)
