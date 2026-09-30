from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("we.page").isin(["/checkout", "/home", "/cart"]))
    .select(
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
    .orderBy("device", "event_time")
)
