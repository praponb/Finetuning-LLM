from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~((F.col("event_type") == "purchase") & F.col("event_type").isin(["search", "click", "page_view"])))
    .withColumn("page_event_type", F.concat("page", F.lit(" "), "event_type"))
)

summary = (
    filtered_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)

result = (
    summary
    .orderBy(F.col("add_to_cart_count").desc(), "session_id")
    .limit(10)
)
