from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "FAILED") & F.col("order_id").isNotNull())
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
    .filter(F.col("paypal_count") > 5)
    .drop("paypal_count")
)
