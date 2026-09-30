from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.substring(F.col("s.carrier"), 1, 4).alias("carrier_prefix"),
    )
)
