from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

kept_keys = (
    df_accounts
    .select("account_type", "id")
    .intersect(
        df_accounts
        .filter(F.col("balance") >= 26000)
        .select("account_type", "id")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
