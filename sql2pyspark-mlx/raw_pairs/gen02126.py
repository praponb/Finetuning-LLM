from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") > "2025-08-01")
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 33, "high").when(F.col("o.shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"),
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", F.col("s.cost")).otherwise(0)).alias("in_transit_cost"),
    )
    .orderBy("shipping_fee_bucket")
)
