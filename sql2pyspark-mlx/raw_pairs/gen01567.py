from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.status") != "COMPLETED") & F.col("o.status").isin(["COMPLETED", "RETURNED", "SHIPPED"]))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("id").desc())
)
