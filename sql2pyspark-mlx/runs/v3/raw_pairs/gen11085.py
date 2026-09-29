from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "RETURNED"]) & F.col("store_id").isin([8, 18, 38]))
    .groupBy("status")
    .agg(
        F.max("discount").alias("max_discount"),
        F.max("order_date").alias("latest_order_date"),
    )
)
