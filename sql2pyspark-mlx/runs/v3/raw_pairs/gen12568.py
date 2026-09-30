from pyspark.sql import functions as F

src = df_web_events.filter(F.hour("event_time").between(12, 20))

web_events_stats = (
    src
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = web_events_stats
