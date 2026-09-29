from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3300)
    .select("id", "is_flagged", "amount", F.length("merchant").alias("merchant_length"))
    .orderBy(F.col("merchant_length").desc())
)
