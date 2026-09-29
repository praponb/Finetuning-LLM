from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.status") == "SHIPPED") & F.col("o.discount").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
)
