from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("shipments.carrier") != "Kerry")
    .groupBy(
        F.when(F.col("orders.shipping_fee") >= 42, "high").when(F.col("orders.shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.count(F.when(F.col("shipments.status") == "RETURNED", 1)).alias("returned_count"),
    )
    .filter(F.col("returned_count") >= 4)
    .orderBy(F.col("returned_count").desc(), "shipping_fee_bucket")
)
