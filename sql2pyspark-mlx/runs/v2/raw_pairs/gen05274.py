from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([17, 33]))
    .groupBy("is_flagged")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
