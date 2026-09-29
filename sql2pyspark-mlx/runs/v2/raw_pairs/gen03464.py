from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("orders.order_date")).alias("order_month"),
        F.col("orders.channel"),
    )
    .agg(
        F.min(F.col("shipments.cost")).alias("min_cost"),
        F.avg(F.col("shipments.cost")).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 66)
)
