from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country").isin(["DE", "TH"]))
    .select(F.col("c.credit_limit"), F.col("s.device"))
)
