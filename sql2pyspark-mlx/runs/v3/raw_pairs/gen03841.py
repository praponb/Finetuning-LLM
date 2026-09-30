from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal", "promptpay"]) & (F.col("method") == "bank_transfer"))
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
    .filter(F.col("paypal_count") >= 77)
    .orderBy("status")
)
