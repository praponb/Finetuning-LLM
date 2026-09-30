from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("p.launch_date").between("2023-03-17", "2025-09-01"))
    .select(
        F.col("oi.quantity"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("o.channel"),
    )
)
