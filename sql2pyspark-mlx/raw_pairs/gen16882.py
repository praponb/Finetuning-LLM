from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .filter(F.col("total_duration_sec") > 2900)
)
