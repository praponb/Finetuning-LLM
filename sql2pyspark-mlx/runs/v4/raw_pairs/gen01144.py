from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.col("c.country") != "DE")
    .groupBy(F.col("c.city"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
    .filter(F.col("unique_countrys") >= 5)
)
