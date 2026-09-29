from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(580, 1050) & (F.col("paid_at") < "2024-07-29") & (F.col("status") != "FAILED"))
    .select("id", "amount", "status", F.floor("amount").alias("amount_whole"))
    .orderBy(F.col("id").desc())
)
