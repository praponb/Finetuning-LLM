from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("status") == "SHIPPED"))
    .groupBy(
        "channel",
        F.when(F.col("shipping_fee") >= 30, "high").when(F.col("shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "store_id",
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)
