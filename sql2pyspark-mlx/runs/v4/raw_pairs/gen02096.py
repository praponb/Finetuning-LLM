from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1070) & (F.col("method") == "bank_transfer"))
    .groupBy("method", "status")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("paypal_count") < 52)
    .orderBy(F.col("method").desc(), "status")
    .limit(10)
)
