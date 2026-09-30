from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2600) & (F.col("amount") >= 1100))
    .groupBy(
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
    .filter(F.col("credit_count") < 4)
    .drop("credit_count")
    .orderBy("amount_bucket")
)
