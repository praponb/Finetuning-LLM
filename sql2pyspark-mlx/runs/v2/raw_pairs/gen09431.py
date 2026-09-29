from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("*").alias("payment_count"),
    )
    .filter(F.col("payment_count") > 2)
    .drop("payment_count")
    .orderBy("amount_range", "order_id")
    .limit(3)
)
