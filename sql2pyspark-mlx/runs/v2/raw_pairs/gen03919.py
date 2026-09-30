from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "bank_transfer", "promptpay"]) | (F.col("order_id") == 24))
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("max_amount") >= 670)
    .orderBy("last_paid_at")
)
