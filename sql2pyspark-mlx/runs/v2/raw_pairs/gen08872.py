from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-08-01")
    .groupBy("is_flagged")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("total_amount") > 12000)
    .select("is_flagged", "total_amount", "avg_amount")
    .orderBy("avg_amount", "is_flagged")
    .limit(50)
)
