from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 52)
    .orderBy("amount_bucket")
)
