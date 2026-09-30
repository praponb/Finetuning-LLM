from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.order_id").isin([8, 14, 23]) & F.col("o.order_date").between("2023-12-01", "2024-12-01"))
    .select(
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.amount"),
    )
)
