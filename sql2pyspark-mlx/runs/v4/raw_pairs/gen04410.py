from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id") == 34)
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.min(F.col("s.started_at")).alias("first_started_at"),
    )
    .orderBy(F.col("first_started_at").desc())
)
