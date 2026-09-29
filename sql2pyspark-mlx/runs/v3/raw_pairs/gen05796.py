from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.month("shipped_date").isin([7, 11, 12]))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.fedex_count") > 35)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("O.fedex_count"),
    )
)
