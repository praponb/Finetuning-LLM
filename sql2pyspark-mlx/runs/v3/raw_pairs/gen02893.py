from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([5, 8, 12]))
    .groupBy(
        F.col("a.status"),
        F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.count(F.when(F.col("t.txn_type") == "DEBIT", 1)).alias("debit_count"))
    .filter(F.col("debit_count") >= 1)
)
