from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 172)
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/cart", "/search", "/product"])
    .agg(F.max("duration_sec"))
)
