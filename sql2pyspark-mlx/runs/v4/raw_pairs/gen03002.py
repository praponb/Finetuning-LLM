from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-01-14 12:00:00").cast("timestamp")) & F.col("order_id").isNotNull() & F.col("method").isin(["bank_transfer", "promptpay", "paypal"]))
    .select("*", F.round((F.col("amount") * 100), 2).alias("amount_scaled"))
)

result = (
    cleaned
    .select(
        "id",
        "method",
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("amount_scaled").alias("AMOUNT_SCALED"),
    )
    .unpivot(["id", "method"], ["AMOUNT_VAL", "AMOUNT_SCALED"], "kpi", "val")
)
