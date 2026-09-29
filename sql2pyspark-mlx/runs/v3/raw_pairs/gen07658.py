from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("min_amount", "status")
    .limit(10)
)
