from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.region").isin(["South", "Central"]) | F.col("o.order_date").between("2024-07-09", "2025-07-01"))
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
