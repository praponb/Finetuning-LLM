from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("O.avg_cost"),
        F.col("O.first_shipped_date"),
    )
    .orderBy(F.col("avg_cost").desc(), "first_shipped_date")
)
