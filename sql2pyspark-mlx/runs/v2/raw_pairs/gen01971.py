from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") < 347) & F.col("session_id").isin([12, 16, 17])) | (F.col("event_type") == "search"))
    .groupBy("page")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") >= 2600)
    .drop("total_duration_sec")
    .orderBy("page")
    .limit(20)
)
