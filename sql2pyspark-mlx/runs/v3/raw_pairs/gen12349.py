from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.dhl_count") >= 6)
    .select(F.col("o.customer_id"), F.col("os.dhl_count"), F.col("os.unique_orders"))
    .orderBy(F.col("unique_orders").desc())
)
