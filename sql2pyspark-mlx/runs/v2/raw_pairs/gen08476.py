from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 4400)
    .select("status", "total_amount")
    .orderBy("status", F.col("total_amount").desc())
)
