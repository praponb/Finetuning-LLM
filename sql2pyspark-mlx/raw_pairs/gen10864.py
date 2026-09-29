from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy(
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
        F.when(F.col("amount") >= 4400, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sum("amount").alias("total_amount"),
    )
)
