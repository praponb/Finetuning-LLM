from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 500, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)
