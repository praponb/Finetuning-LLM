from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~(F.col("event_type").isin(["page_view", "purchase"]) & ~F.col("page").isin(["/product", "/search", "/cart"])))
)

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = agg
