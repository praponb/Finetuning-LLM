from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.unique_merchants"),
        F.col("t.total_count"),
    )
    .orderBy("status")
)
