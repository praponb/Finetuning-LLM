from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.date_add(F.col("s.shipped_date"), 23).alias("shipped_date_plus_23d"),
    )
)
