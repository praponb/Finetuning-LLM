from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .groupBy("event_type")
    .pivot("page", ["/cart", "/checkout"])
    .agg(F.avg("duration_sec"))
    .orderBy("event_type")
)
