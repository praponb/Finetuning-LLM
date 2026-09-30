from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("session_id").isin([11, 17, 23, 28]) & F.col("event_type").isin(["page_view", "search"])) | F.hour("event_time").between(7, 13))
)

agg = (
    recent_web_events
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)

result = (
    agg
    .filter(F.col("add_to_cart_count") >= 10)
    .select("page", "add_to_cart_count")
    .orderBy(F.col("add_to_cart_count").desc())
)
