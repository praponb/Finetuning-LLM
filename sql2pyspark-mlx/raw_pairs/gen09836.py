from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "search"])
    .agg(F.count("duration_sec"))
    .orderBy("page")
)
