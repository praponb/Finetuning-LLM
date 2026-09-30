from pyspark.sql import functions as F

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.device"))
)

result = (
    enriched
    .groupBy(F.date_trunc("quarter", "event_time").alias("quarter_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.min("event_time").alias("started_at"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
