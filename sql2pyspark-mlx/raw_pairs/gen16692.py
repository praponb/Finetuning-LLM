from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .cube("quarter_start")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.count("*").alias("row_count"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .filter(F.col("row_count") > 1)
)
