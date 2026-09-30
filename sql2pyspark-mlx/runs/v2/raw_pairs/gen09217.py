from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.segment").isin(["Consumer", "SMB", "Corporate"]) & (F.col("c.country") == "SG"))
    .select(F.col("c.city"), F.col("c.name"), F.col("s.country"), F.col("s.device"))
    .orderBy(F.col("country").desc())
)
