from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 21)
    .groupBy("event_type")
    .pivot("page", ["/product", "/cart", "/checkout"])
    .agg(F.max("duration_sec"))
    .orderBy("event_type")
)
