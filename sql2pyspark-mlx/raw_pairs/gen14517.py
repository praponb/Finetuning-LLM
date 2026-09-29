from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("status").isNull())
        .select("account_type", "status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "customer_id", "opened_date")
