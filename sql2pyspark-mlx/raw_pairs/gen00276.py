from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") < 3900)
    .orderBy(F.col("min_amount").desc(), "account_id")
    .limit(1)
)
