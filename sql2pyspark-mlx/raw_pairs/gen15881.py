from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/search", "/checkout"]))
    .select("page", "event_type", "event_time")
)
