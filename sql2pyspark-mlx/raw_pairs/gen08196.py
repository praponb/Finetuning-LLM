from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
    .orderBy("merchant", F.col("unique_merchants").desc())
)
