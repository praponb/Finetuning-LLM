from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter((F.col("o.amount") <= 1080) & F.col("s.cost").between(76, 81))
    .select(
        F.col("s.status"),
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
    )
    .orderBy("order_status")
)
