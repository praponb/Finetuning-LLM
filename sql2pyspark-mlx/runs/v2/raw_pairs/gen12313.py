from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("max_amount") >= 3300)
    .orderBy("total_amount", "max_amount")
)
