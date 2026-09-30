from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.grab_amount") > 55)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("t.grab_amount"),
        F.col("t.amazon_count"),
    )
    .orderBy(F.col("amazon_count").desc(), "id")
    .limit(3)
)
