from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("credit_limit") < 10900).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.device").isin(["mobile", "tablet"]))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.device"))
    .orderBy(F.col("device").desc())
)
