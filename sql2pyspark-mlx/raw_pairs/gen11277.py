from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "SAVINGS"]) & (F.col("account_type") == "CHECKING"))
    .select("id", "status", F.round("balance", 0).alias("balance_rounded"))
)
