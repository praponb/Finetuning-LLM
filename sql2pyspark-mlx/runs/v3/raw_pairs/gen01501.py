from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.count(F.when(F.col("t.merchant") == "Amazon", 1)).alias("amazon_count"),
    )
)
