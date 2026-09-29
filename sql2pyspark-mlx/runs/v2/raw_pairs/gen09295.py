from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("page") != "/home"))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
)

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    agg
    .filter(F.col("avg_duration_sec") < 350)
    .select("event_type", "event_type_list", "avg_duration_sec")
    .orderBy("event_type")
    .limit(10)
)
