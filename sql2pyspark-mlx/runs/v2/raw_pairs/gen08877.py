from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 11, 14, 36]) | (F.col("amount") <= 3600))
    .groupBy("is_flagged")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 23000)
)
