from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-04-06") | F.col("session_id").isin([6, 28, 31, 35]))
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), "page")
)
