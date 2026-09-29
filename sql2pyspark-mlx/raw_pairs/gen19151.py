from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
    .orderBy(F.col("event_hour").desc())
    .limit(5)
)
