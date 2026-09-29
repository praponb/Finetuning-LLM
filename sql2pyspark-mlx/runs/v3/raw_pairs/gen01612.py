from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") >= 4)
    .drop("promptpay_count")
)
