from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("cost").between(60, 83))
    .groupBy("order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("os.total_cost"),
        F.col("os.unique_orders"),
    )
)
