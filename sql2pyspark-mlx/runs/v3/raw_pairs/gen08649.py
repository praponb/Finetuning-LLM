from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("started_at") < "2025-04-19").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(~F.col("c.segment").isin(["Corporate", "SMB"]))
    .select(F.col("c.id"), F.col("c.city"))
)
