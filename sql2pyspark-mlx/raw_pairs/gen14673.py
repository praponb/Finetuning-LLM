from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.amount") >= 1270) & (F.col("o.discount") > 0.37))
    .select(
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("o.status"),
        F.col("o.channel"),
    )
    .orderBy("status", F.col("supplier_id").desc())
)
