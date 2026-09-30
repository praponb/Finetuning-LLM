from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([1, 3, 6]))
    .groupBy(F.col("o.channel"))
    .agg(F.max(F.col("s.delivered_date")).alias("last_delivered_date"))
    .orderBy(F.col("last_delivered_date").desc())
)
