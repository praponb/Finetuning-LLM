from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 11000)
)
