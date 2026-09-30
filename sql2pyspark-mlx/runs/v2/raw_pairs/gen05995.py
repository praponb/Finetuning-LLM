from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS"]) & ((F.col("paid_at") >= "2024-07-07") | F.col("order_id").isin([22, 31, 40])))
)

summary = (
    valid_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)

result = summary.filter(F.col("paypal_count") >= 30)
