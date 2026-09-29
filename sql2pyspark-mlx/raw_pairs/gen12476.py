from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("merchant", "amount_count")
)
