from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(
        F.min(F.col("s.delivered_date")).alias("first_delivered_date"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .orderBy("status")
)
