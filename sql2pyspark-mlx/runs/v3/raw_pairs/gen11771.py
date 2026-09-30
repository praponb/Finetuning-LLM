from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]) & F.col("order_id").isNotNull() & ~F.col("method").isin(["paypal", "bank_transfer", "promptpay"]))
    .select(
        "id",
        "method",
        F.upper("method").alias("method_upper"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)
