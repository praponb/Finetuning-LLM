from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() | F.col("discount").between(0.27, 0.36))
    .groupBy(
        "status",
        F.when(F.col("status").isin(["SHIPPED", "CANCELLED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
        F.max("order_date").alias("latest_order_date"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("latest_order_date")
)
