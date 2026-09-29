from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

kept_keys = (
    df_accounts
    .select("account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type") != "LOAN")
        .select("account_type")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "balance", F.col("min_account_balance"))
)
