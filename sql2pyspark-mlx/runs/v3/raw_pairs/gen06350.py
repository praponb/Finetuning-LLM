from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(~(F.col("s.status").isin(["IN_TRANSIT", "DELIVERED", "RETURNED"]) & F.col("o.discount").isNotNull()))
    .select(
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("o.channel"),
        F.col("o.status").alias("order_status"),
        F.col("o.store_id"),
    )
)
