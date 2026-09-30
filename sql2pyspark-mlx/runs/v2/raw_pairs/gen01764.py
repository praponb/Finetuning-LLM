from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date").between("2023-11-01", "2025-01-27"))
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.max(F.col("s.delivered_date")).alias("last_delivered_date"))
)
