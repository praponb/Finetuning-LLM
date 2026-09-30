from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .groupBy("txn_type", "is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") > 2)
    .drop("total_count")
)
