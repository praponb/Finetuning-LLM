from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "paypal") & (F.to_date("paid_at") < "2024-03-01") & ~F.col("status").isin(["REFUNDED", "FAILED"]))
    .groupBy("method")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
    .filter(F.col("min_amount") > 1100)
)
