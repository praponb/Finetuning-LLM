from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(~F.col("o.channel").isin(["store", "partner"]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.ceil(F.col("oi.quantity")).alias("quantity_whole"),
    )
    .orderBy("quantity_whole", "id")
    .limit(5)
)
