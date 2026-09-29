from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]) & F.col("account_type").isin(["SAVINGS", "CHECKING"]))
)

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    agg
    .filter(F.col("balance_range") > 67)
    .select("account_type", "balance_range")
)
