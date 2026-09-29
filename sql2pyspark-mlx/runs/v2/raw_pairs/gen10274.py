from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 15))
    .cube("page", "event_type")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
