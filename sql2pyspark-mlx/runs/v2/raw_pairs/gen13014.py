from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "is_flagged")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1800)
    .orderBy("min_amount")
)
