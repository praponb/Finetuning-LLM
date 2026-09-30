from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("channel", ["web", "partner", "store"])
    .agg(F.avg("discount").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
