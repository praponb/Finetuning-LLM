from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.status"))
    .agg(
        F.round(F.stddev(F.col("shipments.cost")), 2).alias("stddev_cost"),
        F.count(F.when(F.col("shipments.carrier") == "Kerry", 1)).alias("kerry_count"),
    )
    .orderBy(F.col("stddev_cost").desc(), "status")
)
