from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.country").isin(["UK", "JP"]), "priority").otherwise("other").alias("country_group"),
        F.col("s.customer_id"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
    .orderBy("last_event_time")
)
