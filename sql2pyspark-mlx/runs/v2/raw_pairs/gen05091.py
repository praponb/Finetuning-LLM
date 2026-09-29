from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("s.device") == "tablet") & F.col("s.browser").isin(["Firefox", "Safari", "Edge"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.name"),
    )
    .orderBy("name", "device")
)
