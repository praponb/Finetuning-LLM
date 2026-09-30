from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date").between("2023-06-22", "2024-06-01") & F.col("p.cost").between(390, 470))
    .select(
        F.col("p.stock_qty"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
)
