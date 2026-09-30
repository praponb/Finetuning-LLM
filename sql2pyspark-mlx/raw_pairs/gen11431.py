from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("status", "customer_id")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") <= 54000)
        .select("status", "customer_id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("customer_id", "account_type", "balance", "opened_date", "status")
    .orderBy(F.col("status").desc())
)
