from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1110) | (F.col("status") == "FAILED"))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)
