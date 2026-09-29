from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.store_id").isin([1, 5]))
    .select(
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("i.product_id"),
    )
)
