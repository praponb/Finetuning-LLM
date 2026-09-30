from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") <= 4500)
    .groupBy(F.col("c.country"))
    .agg(F.countDistinct(F.col("s.browser")).alias("unique_browsers"))
)
