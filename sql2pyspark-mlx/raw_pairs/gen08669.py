from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.order_date") >= "2025-10-30")
    .groupBy(F.col("o.store_id"), F.col("o.status"))
    .agg(
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
        F.max(F.col("s.delivered_date")).alias("last_delivered_date"),
    )
    .orderBy(F.col("last_delivered_date").desc())
)
