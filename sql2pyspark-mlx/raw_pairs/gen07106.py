from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") >= 40000)
        .select("account_type")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "account_type", "balance", "opened_date", "status")
    .orderBy("balance")
)
