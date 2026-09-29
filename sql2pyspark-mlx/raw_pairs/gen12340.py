from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.hour("event_time").between(11, 17) & (F.col("page") == "/search"))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["add_to_cart", "click"]), "Group A").otherwise("Other"))
)

summary = (
    valid_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    summary
    .filter(F.col("total_duration_sec") < 1300)
    .select("event_day", "total_duration_sec")
)
