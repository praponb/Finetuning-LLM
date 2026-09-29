from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").isNull())
    .select(
        F.col("s.carrier"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("o.status"),
    )
)
