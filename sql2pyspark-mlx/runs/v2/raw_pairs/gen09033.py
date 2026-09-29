from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.month("shipped_date").isin([4, 8, 10])).alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_semi")
    .select(
        F.col("orders.id"),
        F.col("orders.amount"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
    )
)
