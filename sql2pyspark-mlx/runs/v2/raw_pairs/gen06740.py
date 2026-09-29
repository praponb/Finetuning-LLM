from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.avg("amount").alias("avg_amount"))
)
