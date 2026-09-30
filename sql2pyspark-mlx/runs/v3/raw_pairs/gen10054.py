from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "inner")
    .filter(F.col("order_totals.min_cost") > 70)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.shipping_fee"),
        F.col("order_totals.first_shipped_date"),
        F.col("order_totals.min_cost"),
    )
    .orderBy(F.col("id").desc(), "min_cost")
)
