from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
    .orderBy("event_day")
)
