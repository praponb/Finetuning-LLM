from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter((F.col("p.category") != "Grocery") & F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
)
