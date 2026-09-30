from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product"]) & (F.col("page") == "/cart"))
)

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = agg.select("event_type", "min_duration_sec")
