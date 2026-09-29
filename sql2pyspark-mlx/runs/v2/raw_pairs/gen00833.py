from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
)
