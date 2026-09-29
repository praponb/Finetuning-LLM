from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/home", "/checkout", "/product", "/cart"])
    .agg(F.avg("duration_sec"))
    .orderBy("event_type")
)
