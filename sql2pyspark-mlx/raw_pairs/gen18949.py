from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 12) & (F.col("status") == "CANCELLED") & (F.col("customer_id") == 24))
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 19, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("max_shipping_fee") >= 16)
    .orderBy("shipping_fee_bucket")
)
