from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 335)
    .groupBy("event_type")
    .pivot("page", ["/home", "/product", "/search", "/checkout"])
    .agg(F.sum("duration_sec"))
)
