from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.year(F.col("s.delivered_date")) >= 2025) & (F.col("s.cost") < 35))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.store_id"),
    )
)
