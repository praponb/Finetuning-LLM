from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") == 2025) & (F.col("balance") < 72000))
    .groupBy("customer_id")
    .agg(F.count("balance").alias("balance_count"))
    .orderBy(F.col("balance_count").desc())
)
