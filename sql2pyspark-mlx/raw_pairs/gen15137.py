from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.max_cost") > 61)
    .select(
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("O.first_delivered_date"),
        F.col("O.max_cost"),
    )
    .orderBy("channel")
)
