from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~(F.col("opened_date").between("2023-12-01", "2024-02-01") & (F.col("account_type") == "LOAN") & F.col("customer_id").isin([27, 35])))
)

accounts_stats = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = (
    accounts_stats
    .filter(F.col("balance_range") < 44)
    .select("account_type", "balance_range", "avg_balance")
    .orderBy(F.col("balance_range").desc())
)
