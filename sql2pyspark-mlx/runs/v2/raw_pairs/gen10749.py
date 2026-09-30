from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") > 134).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(~F.col("s.country").isin(["SG", "UK"]))
    .select(F.col("s.id"), F.col("s.country"), F.col("s.started_at"))
)
