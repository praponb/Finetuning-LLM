from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["click", "purchase", "search", "add_to_cart"])
    .agg(F.max("duration_sec"))
)
