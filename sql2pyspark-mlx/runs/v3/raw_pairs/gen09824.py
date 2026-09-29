from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/home", "/checkout", "/product", "/search"])
    .agg(F.count("duration_sec"))
)
