from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page").isNotNull()).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(~F.col("s.country").isin(["SG", "TH", "UK"]))
    .select(F.col("s.id"), F.col("s.country"), F.col("s.device"), F.col("s.started_at"))
)
