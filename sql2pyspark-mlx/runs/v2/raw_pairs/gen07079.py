from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") < 399000)
    .orderBy("account_type")
)
