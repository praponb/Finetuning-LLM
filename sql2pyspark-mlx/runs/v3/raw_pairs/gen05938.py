from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("t.unique_statuss"),
        F.col("t.returned_count"),
    )
)
