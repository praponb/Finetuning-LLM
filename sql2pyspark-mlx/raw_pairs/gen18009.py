from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("s.shipped_date")).alias("first_shipped_date"))
)
