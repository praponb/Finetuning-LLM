from pyspark.sql import functions as F

result = (
    df_payments
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
