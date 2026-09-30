from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
    )
    .orderBy(F.col("discount").desc(), "id")
)
