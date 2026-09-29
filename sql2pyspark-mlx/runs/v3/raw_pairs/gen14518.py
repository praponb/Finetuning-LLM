from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.email").isNotNull())
    .groupBy(F.col("c.segment"))
    .agg(F.count("*").alias("account_count"))
    .filter(F.col("account_count") >= 4)
)
