from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id").isin([22, 34]))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "FEE", F.col("t.amount")).otherwise(0)).alias("fee_amount"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .filter(F.col("fee_amount") > 44)
)
