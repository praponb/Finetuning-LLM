from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id") == 15)
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("a.balance")).alias("balance_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("balance_count", "city")
    .limit(5)
)
