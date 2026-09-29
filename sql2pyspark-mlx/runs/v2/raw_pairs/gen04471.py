from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(
        F.col("c.is_active"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
)
