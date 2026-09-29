from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("avg_amount") > 50)
    .select("account_id", "avg_amount", "debit_amount")
    .orderBy(F.col("account_id").desc())
    .limit(5)
)
