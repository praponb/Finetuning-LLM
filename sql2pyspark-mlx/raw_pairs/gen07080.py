from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("balance") >= 67000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        "account_type",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy(F.col("frozen_rows").desc())
)
