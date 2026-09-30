from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "account_id")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 3200)
)
