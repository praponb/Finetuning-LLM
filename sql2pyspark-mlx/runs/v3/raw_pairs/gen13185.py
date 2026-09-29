from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .filter(F.col("orders.store_id").isin([1, 2, 5]))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.min(F.col("shipments.cost")).alias("min_cost"),
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
    .filter(F.col("min_cost") >= 66)
)
