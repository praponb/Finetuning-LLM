from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]) & (F.col("method") == "paypal") & (F.to_date("paid_at") < "2024-12-24"))
    .groupBy("method", "status")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.sum("amount").alias("total_amount"),
    )
)
