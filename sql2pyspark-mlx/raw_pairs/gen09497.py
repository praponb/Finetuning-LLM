from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-06-30"))) & (F.col("method") == "paypal"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.upper("method").alias("method_upper"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

result = (
    base
    .groupBy("method")
    .pivot("status", ["FAILED", "SUCCESS"])
    .agg(F.sum("amount").alias("value_sum"))
)
