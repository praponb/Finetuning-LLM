from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") == "SMB")
    .groupBy(F.col("c.city"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
    .orderBy("city")
    .limit(10)
)
