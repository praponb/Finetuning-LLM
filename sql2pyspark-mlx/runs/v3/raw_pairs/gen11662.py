from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.min("txn_time").alias("first_txn_time"))
    .orderBy(F.col("amount_bucket").desc())
)
