from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal"]) & ((F.col("status") != "REFUNDED") | (F.col("order_id") == 7)))
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)
