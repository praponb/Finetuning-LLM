from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter(~F.col("t.txn_type").isin(["FEE", "REFUND", "CREDIT"]))
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .filter(F.col("avg_amount") >= 1700)
)
