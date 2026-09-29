from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
    .orderBy(F.col("session_count").desc())
)
