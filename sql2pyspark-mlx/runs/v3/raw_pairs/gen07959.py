from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.city") != "Bangkok")
    .groupBy(F.col("c.is_active"))
    .agg(F.countDistinct(F.col("s.device")).alias("unique_devices"))
    .orderBy(F.col("unique_devices").desc())
)
