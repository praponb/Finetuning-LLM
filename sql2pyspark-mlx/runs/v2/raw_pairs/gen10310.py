from pyspark.sql import functions as F

src = df_web_events.filter(F.col("event_type").isin(["add_to_cart", "click"]))

web_events_stats = (
    src
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = web_events_stats
