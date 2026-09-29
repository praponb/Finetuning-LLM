from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.amount").between(990, 1150) & F.col("oi.quantity").between(3, 7))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("o.amount"),
        F.col("o.channel"),
    )
    .orderBy("id")
)
