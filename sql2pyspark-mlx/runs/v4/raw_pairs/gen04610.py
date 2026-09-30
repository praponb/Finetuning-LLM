from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("duration_sec") != 352)

event_month_summary = (
    filtered_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = event_month_summary
