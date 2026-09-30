from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND", "CREDIT"]))
    .groupBy(
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 24000)
)
