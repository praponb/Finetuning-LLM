from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.opened_date") <= "2023-10-27") | F.col("oi.product_id").isin([32, 40]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("region", "id")
    .limit(50)
)
