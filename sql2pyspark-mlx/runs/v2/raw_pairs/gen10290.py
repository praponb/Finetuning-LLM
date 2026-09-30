from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.customer_id").isin([18, 31]))
    .groupBy(F.col("c.is_active"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
    .orderBy(F.col("unique_countrys").desc())
)
