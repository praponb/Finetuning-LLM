from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)
