from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.customer_id"))
    .agg(F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"))
    .orderBy(F.col("customer_id").desc())
    .limit(10)
)
