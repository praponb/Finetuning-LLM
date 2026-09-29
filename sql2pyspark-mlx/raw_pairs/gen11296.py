from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-08").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.round((F.col("amount") * 1.1), 2).alias("amount_scaled"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("method", ["cash", "paypal", "promptpay"])
    .agg(F.avg("amount"))
)
