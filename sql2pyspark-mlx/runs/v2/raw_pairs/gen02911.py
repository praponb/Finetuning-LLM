from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(142, 319))
    .groupBy("event_type")
    .pivot("page", ["/search", "/checkout", "/home", "/product"])
    .agg(F.sum("duration_sec"))
)
