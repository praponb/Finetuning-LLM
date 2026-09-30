from pyspark.sql import functions as F

result = (
    df_payments
    .cube("status", "method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("*").alias("payment_count"),
    )
    .orderBy(F.col("payment_count").desc())
)
