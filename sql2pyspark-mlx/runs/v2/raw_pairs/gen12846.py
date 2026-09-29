from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 12, 19]) | F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.sum("amount").alias("total_amount"))
)
