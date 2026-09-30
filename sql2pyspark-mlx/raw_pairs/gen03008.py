from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_type").isin(["CREDIT", "DEBIT", "REFUND"]) | F.col("is_flagged")))
)

summary = (
    filtered_transactions
    .groupBy(
        F.to_date("txn_time").alias("txn_day"),
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    summary
    .select("txn_day", "amount_bucket", "total_amount")
    .orderBy("amount_bucket", F.col("txn_day").desc())
)
