from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1200)
    .groupBy("merchant")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") >= 9000)
    .orderBy("merchant", "total_amount")
)
