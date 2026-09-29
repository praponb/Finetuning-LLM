from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~((F.col("status") == "CLOSED") & (F.col("opened_date") <= "2024-12-23")))
    .withColumn("balance_rounded", F.round("balance", 0))
)

accounts_stats = (
    valid_accounts
    .groupBy("status", "account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)

result = accounts_stats
