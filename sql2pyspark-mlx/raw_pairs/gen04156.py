from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.started_at") < "2025-05-28") & F.lower(F.col("c.name")).like("%vy %"))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("c.age"), F.col("c.country"))
)
