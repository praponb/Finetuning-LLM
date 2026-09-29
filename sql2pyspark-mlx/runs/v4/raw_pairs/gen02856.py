from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-10-01")
    .groupBy("order_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.cost_range") > 36)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("ot.cost_range"),
    )
    .orderBy("id")
    .limit(10)
)
