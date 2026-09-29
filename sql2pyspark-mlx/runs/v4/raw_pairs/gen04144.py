from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("amount_scaled", F.round((F.col("amount") * 0.9), 2))
)

payments_stats = (
    base
    .groupBy(
        "method",
        F.when(F.col("amount_scaled") >= 73, "high").when(F.col("amount_scaled") >= 32, "mid").otherwise("low").alias("amount_scaled_bucket"),
    )
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.count("amount_scaled").alias("amount_scaled_count"),
    )
)

result = (
    payments_stats
    .filter(F.col("amount_scaled_count") < 8)
    .select("method", "amount_scaled_bucket", "paypal_count", "amount_scaled_count")
)
