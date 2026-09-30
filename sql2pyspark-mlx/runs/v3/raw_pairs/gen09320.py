from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("customer_id").isin([12, 15, 28])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.duration_sec") != 319)
    .select(F.col("we.id"), F.col("we.duration_sec"))
    .orderBy("id")
)
