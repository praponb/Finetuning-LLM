from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("country") == "SG").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.started_at") >= "2024-05-01")
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.device"))
    .orderBy("device")
)
