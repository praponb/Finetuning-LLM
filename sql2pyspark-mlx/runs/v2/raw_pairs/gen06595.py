from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() | (F.col("balance") >= 77000))
    .groupBy("account_type")
    .agg(F.max("balance").alias("max_balance"))
)
