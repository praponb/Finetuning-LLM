from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "store_id")
    .pivot("status", ["CANCELLED", "SHIPPED"])
    .agg(F.avg("amount"))
    .select(
        "customer_id",
        "store_id",
        F.col("CANCELLED").alias("cancelled"),
        F.col("SHIPPED").alias("shipped"),
    )
)
