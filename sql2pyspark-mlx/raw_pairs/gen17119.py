from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("segment").isin(["Consumer", "SMB"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.started_at") < "2023-04-01")
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.customer_id"))
)
