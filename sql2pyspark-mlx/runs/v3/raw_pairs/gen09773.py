from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
        F.year(F.col("a.opened_date")).alias("opened_year"),
    )
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy(F.col("amount_count").desc(), "opened_month")
)
