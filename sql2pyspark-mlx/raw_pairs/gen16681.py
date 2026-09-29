from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.delivered_date").between("2024-03-26", "2025-09-02"))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("o.status").alias("order_status"),
    )
)
