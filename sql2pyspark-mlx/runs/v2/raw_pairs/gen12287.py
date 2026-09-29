from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("s.country").isin(["JP", "TH"]))
    .groupBy(F.col("s.device"))
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
    .orderBy("device")
)
