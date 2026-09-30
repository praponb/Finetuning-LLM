from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["RETURNED", "SHIPPED", "PENDING"]) & F.col("store_id").isNotNull() & F.col("status").isin(["CANCELLED", "SHIPPED", "COMPLETED"]))
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy("channel")
)
