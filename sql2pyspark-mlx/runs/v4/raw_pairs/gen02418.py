from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
    .filter(F.col("success_amount") >= 74)
    .orderBy("status", F.col("success_amount").desc())
)
