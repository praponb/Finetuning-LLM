from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.year("event_time").alias("event_year"),
        F.month("event_time").alias("event_month"),
        "page",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
    .orderBy("page")
)
