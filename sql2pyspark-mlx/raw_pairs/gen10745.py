from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.country").alias("country"),
        F.col("a.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.duration_sec")), 2).alias("total_duration_sec"),
    )
    .orderBy(F.col("country").desc_nulls_last())
)
