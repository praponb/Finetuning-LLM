from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec").between(333, 358))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("we.event_type") == "add_to_cart", F.col("we.duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
    .orderBy("web_event_count")
)
