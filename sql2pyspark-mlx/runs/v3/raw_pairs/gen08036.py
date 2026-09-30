from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
        F.min(F.col("s.delivered_date")).alias("first_delivered_date"),
    )
    .orderBy("first_delivered_date", "status")
)
