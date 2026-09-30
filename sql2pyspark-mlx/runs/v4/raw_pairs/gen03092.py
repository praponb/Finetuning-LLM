from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
    .orderBy("segment")
)
