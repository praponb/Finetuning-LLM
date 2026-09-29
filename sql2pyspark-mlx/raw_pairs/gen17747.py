from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("unique_accounts").desc(), F.col("amount_range").desc())
)
