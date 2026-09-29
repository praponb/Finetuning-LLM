from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.coalesce(F.col("o.channel"), F.lit("N/A")).alias("channel_filled"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)
