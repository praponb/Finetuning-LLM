from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 19) & F.col("method").isin(["paypal", "promptpay", "cash"]))
    .select(
        "order_id",
        "amount",
        "status",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy("order_id")
)
