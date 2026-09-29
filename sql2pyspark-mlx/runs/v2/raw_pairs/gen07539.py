from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("status") == "COMPLETED") | (F.col("store_id") == 10)))
    .groupBy(
        "status",
        F.month("order_date").alias("order_month"),
        F.when(F.col("shipping_fee") >= 36, "high").when(F.col("shipping_fee") >= 20, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.max("order_date").alias("last_order_date"),
    )
    .orderBy("pending_count")
)
