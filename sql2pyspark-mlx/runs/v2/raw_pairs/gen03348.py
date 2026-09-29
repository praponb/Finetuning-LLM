from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.carrier").isNotNull())
    .select(
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("o.customer_id"),
        F.col("o.discount"),
    )
    .orderBy(F.col("customer_id").desc())
)
