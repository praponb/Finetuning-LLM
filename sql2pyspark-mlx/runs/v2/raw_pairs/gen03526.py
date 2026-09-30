from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
    )
    .filter(F.col("unique_txn_types") >= 1)
    .orderBy(F.col("total_amount").desc())
)
