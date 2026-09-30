from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.city").isNull())
    .select(
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("customer_id").desc())
)
