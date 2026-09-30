from pyspark.sql import functions as F

result = (
    df_payments
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
