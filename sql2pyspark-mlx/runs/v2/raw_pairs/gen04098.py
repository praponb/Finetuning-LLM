from pyspark.sql import functions as F

src = df_orders.filter((F.col("discount") >= 0.12) | (F.col("customer_id") == 32))

agg = (
    src
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 14, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "status",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
    )
)

result = (
    agg
    .select("shipping_fee_bucket", "status", "pending_shipping_fee")
    .orderBy("pending_shipping_fee", F.col("status").desc(), "shipping_fee_bucket")
    .limit(5)
)
