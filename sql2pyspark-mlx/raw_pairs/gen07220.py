from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([8, 40]) & F.col("we.session_id").isin([14, 20, 35]))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
        F.col("s.device"),
    )
    .orderBy("customer_id")
)
