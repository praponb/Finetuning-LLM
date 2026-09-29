from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("status").isin(["CLOSED", "FROZEN"]))

account_type_summary = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = (
    account_type_summary
    .select("account_type", "balance_count", "avg_balance")
    .orderBy(F.col("balance_count").desc())
)
