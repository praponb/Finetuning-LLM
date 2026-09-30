from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN"]) & (~F.col("account_type").isin(["CREDIT", "LOAN"]) | (F.col("status") == "FROZEN")))
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
