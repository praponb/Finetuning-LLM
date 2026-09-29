from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("txn_type").isin(["DEBIT", "FEE"]))

agg = (
    filtered_transactions
    .groupBy(
        F.when(F.col("amount") >= 3100, "high").when(F.col("amount") >= 1800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    agg
    .filter(F.col("total_amount") > 14000)
    .select("amount_bucket", "total_amount")
    .orderBy(F.col("total_amount").desc())
)
