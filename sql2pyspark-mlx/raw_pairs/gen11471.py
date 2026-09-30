from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2024-07-24") & (F.col("customer_id").isNull() | (F.col("balance") <= 18000)))
    .select("id", "account_type", F.floor("balance").alias("balance_whole"))
)
