from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max("amount").alias("max_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
    .orderBy(F.col("max_amount").desc())
)
