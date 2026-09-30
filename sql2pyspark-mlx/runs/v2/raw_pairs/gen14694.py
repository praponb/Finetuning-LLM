from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 19) & F.col("method").isin(["paypal", "card", "promptpay"]))
    .groupBy("status", "order_id", "method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("num_payments"),
    )
    .orderBy("status")
)
