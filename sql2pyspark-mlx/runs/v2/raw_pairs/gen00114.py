from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("paypal_count") >= 25)
)
