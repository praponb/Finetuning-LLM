from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("amount_range").desc())
)
