from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country") == "TH")
    .groupBy(F.col("c.country"))
    .agg(
        F.min(F.col("s.started_at")).alias("first_started_at"),
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
)
