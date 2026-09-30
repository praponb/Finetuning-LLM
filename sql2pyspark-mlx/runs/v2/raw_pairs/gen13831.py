from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.channel") != "store")
    .groupBy(F.date_format(F.col("orders.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg(F.col("shipments.cost")), 2).alias("avg_cost"),
        F.max(F.col("shipments.shipped_date")).alias("last_shipped_date"),
    )
)
