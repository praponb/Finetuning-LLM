from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 7400)
)
