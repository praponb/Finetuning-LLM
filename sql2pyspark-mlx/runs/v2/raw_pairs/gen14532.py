from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .filter(F.col("order_summary.max_cost") > 93)
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.discount"),
        F.col("order_summary.last_shipped_date"),
        F.col("order_summary.max_cost"),
    )
    .orderBy("last_shipped_date", "discount")
)
