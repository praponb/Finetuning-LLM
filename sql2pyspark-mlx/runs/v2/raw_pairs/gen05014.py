from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
    .orderBy(F.col("id").desc())
)
