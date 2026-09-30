from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.col("s.carrier") == "Kerry") & F.col("o.store_id").isNull())
    .select(
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("o.order_date"),
    )
)
