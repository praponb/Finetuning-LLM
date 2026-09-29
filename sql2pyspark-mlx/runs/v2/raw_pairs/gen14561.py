from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 15)
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3400, "high").when(F.col("amount") >= 800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
    )
    .filter(F.col("pct_debit") >= 74)
    .select("txn_type", "amount_bucket", "total_amount", "pct_debit")
    .orderBy("total_amount")
)
