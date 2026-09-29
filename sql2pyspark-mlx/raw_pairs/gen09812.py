from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Amazon")
        .select("id", "merchant")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("txn_type", "txn_time", "merchant")
    .orderBy(F.col("txn_time").asc_nulls_last())
)
