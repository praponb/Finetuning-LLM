from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1250)
    .orderBy(F.col("status").desc())
)
