from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.shipped_date") > "2024-03-21")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.max(F.col("s.delivered_date")).alias("latest_delivered_date"),
    )
)
