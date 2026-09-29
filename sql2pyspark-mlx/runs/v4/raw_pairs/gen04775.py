from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-07-28")
    .groupBy("event_type", F.month("event_time").alias("event_month"))
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)
