from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.credit_limit") <= 9900)
    .groupBy(F.col("c.id"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
    .orderBy("id")
)
