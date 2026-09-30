from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(F.count("*").alias("total_count"))
    .orderBy("total_count", "is_active")
    .limit(3)
)
