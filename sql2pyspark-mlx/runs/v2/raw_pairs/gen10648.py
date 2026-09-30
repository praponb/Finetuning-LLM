from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .pivot("event_type", ["purchase", "page_view", "add_to_cart"])
    .agg(F.sum("duration_sec"))
    .orderBy("event_year")
)
