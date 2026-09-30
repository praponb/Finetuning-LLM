from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "add_to_cart"]) | (F.col("event_type") == "click"))
    .select("duration_sec")
    .orderBy("duration_sec")
)
