from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.last_restock") < "2025-01-17")
    .select(
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.round((F.col("p.stock_qty") / F.col("p.price")), 2).alias("stock_qty_per_price"),
    )
)
