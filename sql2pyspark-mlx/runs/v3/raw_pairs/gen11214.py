from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 13, 21, 34]) & ~F.col("is_flagged"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
