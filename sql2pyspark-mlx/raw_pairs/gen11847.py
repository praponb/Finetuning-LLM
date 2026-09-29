from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "bank_transfer", "cash"]) & ((F.col("method") == "promptpay") | ~F.col("status").isin(["REFUNDED", "SUCCESS"])))
    .groupBy("method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .orderBy(F.col("stddev_amount").desc(), "unique_statuss", "method")
    .limit(5)
)
