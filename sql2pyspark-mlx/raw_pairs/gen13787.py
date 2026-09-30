from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

matched_keys = (
    df_accounts
    .select("status", "id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([17, 28]))
        .select("status", "id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        F.when(F.col("account_type").rlike("^(CREDIT|SAVINGS)$"), "CORE").otherwise("GROUP_B").alias("account_type_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("account_type_zone").desc_nulls_last())
)
