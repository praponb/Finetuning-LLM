from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Lazada"]))
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy("avg_amount")
)
