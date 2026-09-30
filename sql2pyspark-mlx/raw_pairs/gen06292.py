from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-09-13 22:00:00"))) & F.col("page").isNotNull())
    .withColumn("years_since_event_time", (F.datediff(F.current_date(), "event_time") / 365))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("session_id")
    .pivot("page", ["/cart", "/search"])
    .agg(F.max("duration_sec").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "session_id",
        F.col("/cart_amount").alias("cart_amount"),
        F.col("/cart_item_count").alias("cart_item_count"),
        F.col("/search_amount").alias("search_amount"),
        F.col("/search_item_count").alias("search_item_count"),
    )
)
