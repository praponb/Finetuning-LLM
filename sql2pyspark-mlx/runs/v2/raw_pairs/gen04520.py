from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.device") == "mobile")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.country"))
)
