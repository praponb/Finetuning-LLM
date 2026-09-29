from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & (F.col("account_type") == "CREDIT"))
    .groupBy("status", "account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 74)
    .orderBy("balance_range", "status")
)
