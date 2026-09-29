from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
    )
    .filter(F.col("checkout_duration_sec") < 27)
)
