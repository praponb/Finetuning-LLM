from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Grab"]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(F.col("a.balance"), F.col("at.lazada_count"))
    .orderBy(F.col("balance").desc())
)
