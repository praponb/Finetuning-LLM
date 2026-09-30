from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 21000)
    .select("is_flagged", "total_amount")
)
