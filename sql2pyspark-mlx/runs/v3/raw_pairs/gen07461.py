from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .filter(F.col("o.order_date") >= "2025-10-01")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("order_id").desc())
)
