from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2400, 3900))
    .cube(
        "txn_type",
        F.when(F.col("amount") >= 3400, "high").when(F.col("amount") >= 1900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
