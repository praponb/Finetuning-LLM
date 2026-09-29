from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-03-01") & (F.col("amount") <= 1580))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .filter(F.col("failed_amount") >= 35)
)
