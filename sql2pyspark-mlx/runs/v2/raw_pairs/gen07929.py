from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("t.cost_count"),
    )
    .orderBy(F.col("cost_count").desc(), "order_date")
)
