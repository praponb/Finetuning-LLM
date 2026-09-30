from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "promptpay") & F.col("method").isNull())
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
