from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("we.duration_sec").between(223, 448) | F.col("c.name").like("%Park%"))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)
