from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .groupBy("event_type")
    .pivot("page", ["/cart", "/home", "/search"])
    .agg(F.max("duration_sec"))
    .orderBy("event_type")
)
