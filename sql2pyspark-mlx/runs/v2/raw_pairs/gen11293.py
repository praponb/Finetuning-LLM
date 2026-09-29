from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-01-01")
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("success_amount").desc())
)
