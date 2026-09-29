from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .groupingSets(
        [["status", "txn_type"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("a.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
