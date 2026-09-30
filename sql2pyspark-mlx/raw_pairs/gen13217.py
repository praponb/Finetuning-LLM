from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device").isin(["desktop", "mobile"]))
    .groupBy(F.col("s.device"))
    .agg(
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
    )
    .orderBy("device")
)
