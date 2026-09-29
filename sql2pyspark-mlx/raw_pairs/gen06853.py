from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout"]))
    .groupBy("page")
    .pivot("event_type", ["page_view", "search"])
    .agg(F.count("duration_sec"))
    .orderBy("page")
)
