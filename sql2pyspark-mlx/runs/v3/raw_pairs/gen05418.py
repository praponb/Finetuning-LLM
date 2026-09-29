from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.min("balance").alias("min_balance"))
    .filter(F.col("min_balance") > 63000)
    .select("account_type", "min_balance")
)
