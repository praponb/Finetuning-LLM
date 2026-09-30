from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 2300)
    .orderBy("txn_type")
)
