from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.col("oi.unit_price").between(260, 380))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.round((F.col("oi.unit_price") / F.col("oi.quantity")), 2).alias("line_total"),
    )
    .orderBy("price")
)
