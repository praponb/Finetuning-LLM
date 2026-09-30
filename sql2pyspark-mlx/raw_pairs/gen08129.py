from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
        F.sum(F.when(F.col("we.page") == "/home", F.col("we.duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
    .orderBy("segment")
)
