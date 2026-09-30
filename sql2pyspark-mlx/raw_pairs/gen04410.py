from pyspark.sql import functions as F

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .cube("year_start", "quarter_start")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("quarter_start")
)
