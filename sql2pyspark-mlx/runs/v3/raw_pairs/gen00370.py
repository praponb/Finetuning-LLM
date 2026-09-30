from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(
        F.when(F.col("a.status").isin(["CLOSED", "FROZEN"]), "tier_1").otherwise("other").alias("status_group"),
        F.when(F.col("a.balance") >= 79000, "high").when(F.col("a.balance") >= 7000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"))
    .orderBy(F.col("status_group").desc())
)
