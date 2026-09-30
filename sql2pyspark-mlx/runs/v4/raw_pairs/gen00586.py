from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.amount"), F.col("o.discount"), F.col("t.fedex_count"))
    .orderBy(F.col("fedex_count").desc())
)
