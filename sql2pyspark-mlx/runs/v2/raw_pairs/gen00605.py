from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 36000) & (F.col("balance") >= 58000) & F.col("customer_id").isNull())
    .groupBy("status")
    .agg(F.count("balance").alias("balance_count"))
    .orderBy(F.col("balance_count").desc())
)
