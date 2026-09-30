from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
    )
    .orderBy("event_type_values", "country")
    .limit(1)
)
