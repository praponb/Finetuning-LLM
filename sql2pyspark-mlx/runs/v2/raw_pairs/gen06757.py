from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .pivot("page", ["/cart", "/home", "/checkout", "/product"])
    .agg(F.count("duration_sec"))
    .orderBy("event_month")
)
