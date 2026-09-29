from pyspark.sql import functions as F

enriched = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.balance"), F.col("a.status"))
)

result = (
    enriched
    .withColumn("balance_tier", F.when(F.col("balance") >= 80000, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .groupingSets(
        [["balance_tier", "activity_month"], ["balance_tier"]],
        "balance_tier",
        "activity_month",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("last_seen"),
        F.grouping_id().alias("grouping_level"),
    )
)
