from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 76)
    .select("account_type", "balance_range")
    .orderBy(F.col("balance_range").desc(), "account_type")
    .limit(10)
)
