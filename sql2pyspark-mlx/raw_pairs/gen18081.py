from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price").between(280, 340)).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("o.order_date"),
    )
)
