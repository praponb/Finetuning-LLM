from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "search", "add_to_cart"]))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
