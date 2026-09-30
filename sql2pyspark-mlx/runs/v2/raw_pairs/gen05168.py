from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.cost").between(320, 510))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("discount").desc(), "id")
    .limit(3)
)
