from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("c.country").isin(["SG", "UK", "TH"]))
    .groupBy(F.col("s.browser"))
    .agg(
        F.sum(F.when(F.col("we.event_type") == "page_view", F.col("we.duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .orderBy(F.col("duration_sec_count").desc(), "page_view_duration_sec")
)
