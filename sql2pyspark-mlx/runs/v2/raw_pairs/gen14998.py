from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 3700)
    .select("merchant", "min_amount")
    .orderBy(F.col("merchant").desc(), "min_amount")
    .limit(50)
)
