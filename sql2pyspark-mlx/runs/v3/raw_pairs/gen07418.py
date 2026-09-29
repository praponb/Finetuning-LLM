from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") != "add_to_cart") & (F.col("event_time") < "2025-05-01"))
    .groupBy("event_type")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
    .orderBy("event_type")
)
