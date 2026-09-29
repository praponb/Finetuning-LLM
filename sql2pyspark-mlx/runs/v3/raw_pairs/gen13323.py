from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "txn_type",
        "merchant",
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
    .orderBy("txn_type", "amount_bucket")
)
