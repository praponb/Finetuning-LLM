from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "search", "page_view"]) | F.col("page").isin(["/cart", "/search", "/home"]))
)

agg = (
    filtered_web_events
    .groupBy("page")
    .agg(F.max("event_time").alias("last_event_time"))
)

result = agg.select("page", "last_event_time")
