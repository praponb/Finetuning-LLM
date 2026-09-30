from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
    )
    .orderBy(F.col("unique_customers").desc(), "id", "segment")
    .limit(50)
)
