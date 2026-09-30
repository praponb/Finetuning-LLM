from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("id").desc())
)
