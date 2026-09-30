from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") < 361000)
    .orderBy(F.col("account_type").desc())
)
