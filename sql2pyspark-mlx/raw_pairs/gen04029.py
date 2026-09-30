from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.device").isin(["tablet", "mobile", "desktop"]))
    .select(
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("c.country"),
    )
)
