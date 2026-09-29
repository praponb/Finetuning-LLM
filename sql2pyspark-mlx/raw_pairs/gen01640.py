from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("store_id") == 9).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.product_id").isin([7, 25]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
    .orderBy("id")
)
