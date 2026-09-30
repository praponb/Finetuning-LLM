from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 12))
    .groupBy("order_id", "method")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .filter(F.col("success_count") >= 8)
)
