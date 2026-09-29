from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("s.browser"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/search", F.col("we.duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy(F.col("search_duration_sec").desc())
)
