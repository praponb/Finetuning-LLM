from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method").isin(["card", "cash", "paypal"]) & F.col("order_id").isNull()) | F.col("order_id").isin([3, 14, 15, 24]))
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("amount_count") > 3)
)
