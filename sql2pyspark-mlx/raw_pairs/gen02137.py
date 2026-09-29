from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.customer_id").isin([4, 25, 37, 38]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
)
