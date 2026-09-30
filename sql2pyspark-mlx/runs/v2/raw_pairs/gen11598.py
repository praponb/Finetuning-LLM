from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isin([10, 21, 32]))
    .groupBy(F.col("c.country"))
    .agg(F.max(F.col("s.started_at")).alias("latest_started_at"))
)
