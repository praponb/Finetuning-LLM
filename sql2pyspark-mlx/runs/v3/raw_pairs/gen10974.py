from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-10-23") & (F.col("session_id") == 12))
    .rollup("page", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.count("*").alias("total_count"),
    )
)
