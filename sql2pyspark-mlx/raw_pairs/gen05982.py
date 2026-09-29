from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([13, 28, 29]))
    .groupBy(
        F.when(F.col("s.device").isin(["desktop", "mobile"]), "priority").otherwise("other").alias("device_group"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
    )
)
