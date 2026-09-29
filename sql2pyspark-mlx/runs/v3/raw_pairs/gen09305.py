from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .filter(F.hour(F.col("s.started_at")).between(12, 19))
    .groupBy(
        F.col("s.country"),
        F.quarter(F.col("s.started_at")).alias("started_quarter"),
    )
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") > 35)
)
