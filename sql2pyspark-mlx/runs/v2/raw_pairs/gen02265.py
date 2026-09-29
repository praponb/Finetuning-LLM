from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .filter(F.col("failed_amount") < 40)
    .orderBy(F.col("status").desc())
)
