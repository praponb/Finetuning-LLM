from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/product", "/cart", "/checkout", "/search"])
    .agg(F.count("duration_sec"))
    .orderBy("event_type")
)
