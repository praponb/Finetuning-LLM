from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.price") <= 930)
    .select(
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("i.quantity"),
    )
)
