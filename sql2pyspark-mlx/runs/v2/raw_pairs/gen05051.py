from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter((F.col("t.txn_time") >= "2023-03-10") | F.col("t.amount").between(1500, 2300))
    .select(
        F.col("a.opened_date"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("t.txn_time").cast("date").alias("txn_time_day"),
    )
    .orderBy("txn_time_day")
)
