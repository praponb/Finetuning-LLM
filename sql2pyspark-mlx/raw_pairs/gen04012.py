from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("fee_count") > 6)
    .select("account_id", "fee_count", "avg_amount")
    .orderBy(F.col("account_id").desc())
    .limit(20)
)
