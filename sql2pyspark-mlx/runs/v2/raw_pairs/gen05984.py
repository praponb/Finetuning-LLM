from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 3600)
    .drop("total_amount")
    .orderBy(F.col("amount_count").desc(), F.col("method").desc())
)
