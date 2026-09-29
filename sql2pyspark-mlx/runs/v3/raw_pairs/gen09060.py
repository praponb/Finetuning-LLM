from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select("amount", F.length("status").alias("status_length"))
    .orderBy(F.col("amount").desc())
)
