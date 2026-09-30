from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("t.txn_type").isin(["CREDIT", "FEE"]))
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", F.col("t.amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("debit_amount") > 48)
    .orderBy(F.col("stddev_amount").desc())
)
