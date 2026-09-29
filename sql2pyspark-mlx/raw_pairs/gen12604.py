from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["CANCELLED", "COMPLETED"]), "tier_1").otherwise("other").alias("status_group"),
        "status",
    )
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") > 34)
    .select("status_group", "status", "last_order_date", "min_shipping_fee")
    .orderBy(F.col("status_group").desc(), "status")
    .limit(10)
)
