from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.hour("event_time").between(11, 18) | F.col("duration_sec").between(160, 214))
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 426, "High").when(F.col("duration_sec") >= 74, "Medium").otherwise("Low"))
)

agg = (
    valid_web_events
    .groupBy("event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    agg
    .filter(F.col("row_count") < 5)
    .select("event_type", "page_values")
    .orderBy("event_type")
)
