from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("method").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
    )
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("method", ["card", "paypal"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
