from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/product", "/search", "/cart"])
    .agg(F.sum("duration_sec"))
    .orderBy("event_type")
)
