from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("balance") < 67000)
        .select("id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "account_type", "status")
    .orderBy(F.col("id").desc_nulls_first())
)
