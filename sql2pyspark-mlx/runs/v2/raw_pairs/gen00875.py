from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(~F.col("s.browser").isin(["Chrome", "Safari", "Edge"]))
    .groupBy(F.col("s.device"))
    .agg(
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
)
