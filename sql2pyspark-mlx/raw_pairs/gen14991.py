from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "card", "promptpay"]))
    .select(
        "id",
        "order_id",
        "paid_at",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.length("method").alias("method_length"),
    )
    .orderBy("status_method_key", "id")
    .limit(1)
)
