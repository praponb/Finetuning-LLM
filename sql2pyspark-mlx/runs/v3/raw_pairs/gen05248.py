from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") <= "2024-12-01")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("o.status"),
        F.col("o.channel"),
    )
)
