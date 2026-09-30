from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

kept_keys = (
    df_subscriptions
    .select("id", "status")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("id", "status")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "EXTENDED").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("monthly_fee").desc_nulls_first())
)
