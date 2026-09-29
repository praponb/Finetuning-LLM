from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([19, 20]))
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
