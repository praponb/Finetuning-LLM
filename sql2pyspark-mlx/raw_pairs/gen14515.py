from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.order_date").between("2024-05-01", "2025-07-23") | ~F.col("o.channel").isin(["partner", "mobile", "store"]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
)
