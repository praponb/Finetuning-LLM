from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & (F.col("order_id") == 39) & F.col("order_id").isin([3, 36, 39]))
    .groupBy(
        F.when(F.col("amount") >= 1340, "high").when(F.col("amount") >= 560, "mid").otherwise("low").alias("amount_bucket"),
        "order_id",
        "method",
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy(F.col("method").desc(), F.col("amount_bucket").desc())
)
