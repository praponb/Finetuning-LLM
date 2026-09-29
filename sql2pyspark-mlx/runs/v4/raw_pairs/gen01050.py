from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

remaining_keys = (
    df_accounts
    .select("customer_id")
    .intersect(
        df_accounts
        .filter(F.col("balance").between(59000, 77000))
        .select("customer_id")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "opened_date", "status", F.col("avg_account_balance"))
    .orderBy(F.col("avg_account_balance").desc_nulls_last(), "status")
)
