from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([2, 13]) & (F.col("s.started_at") >= "2023-12-30"))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("c.age"))
)
