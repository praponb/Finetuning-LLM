from pyspark.sql import functions as F

base = df_transactions.filter(F.col("merchant") == "7-Eleven")

merchant_summary = (
    base
    .groupBy("merchant")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    merchant_summary
    .filter(F.col("max_amount") < 1200)
    .orderBy("merchant")
)
