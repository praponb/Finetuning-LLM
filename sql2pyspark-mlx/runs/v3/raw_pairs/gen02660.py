from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.discount"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("s.cost"),
    )
    .orderBy(F.col("shipped_date").desc())
)
