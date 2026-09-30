from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id") == 21).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.customer_id") == 30)
    .select(F.col("s.id"), F.col("s.customer_id"))
)
