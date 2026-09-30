from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.delivered_date").between("2023-10-10", "2025-01-01"))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("o.discount"),
    )
)
