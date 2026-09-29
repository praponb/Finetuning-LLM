from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.device").isin(["tablet", "desktop"]))
    .groupBy(F.quarter(F.col("s.started_at")).alias("started_quarter"))
    .agg(
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
    )
)
