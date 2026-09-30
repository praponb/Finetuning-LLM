from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 620)
    .select("paid_at", F.ceil("amount").alias("amount_whole"))
    .orderBy(F.col("amount_whole").desc())
)
