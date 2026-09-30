from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
