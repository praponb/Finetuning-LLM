from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(
        F.col("o.status"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.order_id"),
    )
    .orderBy(F.col("order_id").desc(), F.col("store_id").desc())
)
