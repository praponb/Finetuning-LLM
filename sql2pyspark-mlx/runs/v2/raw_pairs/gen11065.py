from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.opened_date") >= "2023-04-01")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.when(F.col("t.merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 5)
    .orderBy("amount_count", "account_type")
    .limit(20)
)
