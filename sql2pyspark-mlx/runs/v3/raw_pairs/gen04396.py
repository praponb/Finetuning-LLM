from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_merchants"),
    )
    .filter(F.col("transaction_count") > 3)
    .orderBy(F.col("id").desc())
)
