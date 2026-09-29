from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 2000)
    .drop("avg_amount")
)
