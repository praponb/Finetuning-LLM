from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("country").isin(["TH", "JP", "UK"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.device"), F.col("s.browser"), F.col("s.started_at"))
    .orderBy(F.col("device").desc())
)
