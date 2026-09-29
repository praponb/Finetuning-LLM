from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "page_view"]))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
    )
)
