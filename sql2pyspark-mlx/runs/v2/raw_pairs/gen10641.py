from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") != 472) & F.col("event_type").isin(["purchase", "page_view"]))
    .select(
        "event_time",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
    )
    .orderBy("is_checkout")
)
