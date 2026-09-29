from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & F.col("p.method").isin(["cash", "paypal", "bank_transfer"]))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.discount"),
        F.when(F.col("p.amount") >= 1370, "High").when(F.col("p.amount") >= 460, "Medium").otherwise("Low").alias("amount_tier"),
    )
    .orderBy(F.col("amount_tier").desc(), "id")
    .limit(5)
)
