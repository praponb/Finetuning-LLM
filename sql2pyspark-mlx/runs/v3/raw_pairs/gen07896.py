from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.shipped_date").between("2023-06-01", "2023-07-11"))
    .groupBy(F.col("o.status"))
    .agg(F.count(F.col("s.id")).alias("shipment_count"))
    .orderBy(F.col("status").desc())
)
