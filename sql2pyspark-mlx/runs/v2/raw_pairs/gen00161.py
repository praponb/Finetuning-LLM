from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("is_flagged")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 3200)
    .orderBy("min_amount")
)
