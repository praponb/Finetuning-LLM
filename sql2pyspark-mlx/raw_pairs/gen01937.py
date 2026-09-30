from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("id", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "account_type")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("customer_id", "balance")
    .orderBy("balance")
)
