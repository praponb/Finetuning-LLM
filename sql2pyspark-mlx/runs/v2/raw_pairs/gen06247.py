from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id") == 26).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(~F.col("s.country").isin(["SG", "JP", "TH"]))
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.started_at"))
)
