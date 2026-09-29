from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount").between(2300, 3900))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("tba.avg_amount"),
        F.col("tba.cnt"),
    )
    .orderBy(F.col("balance").desc())
)
