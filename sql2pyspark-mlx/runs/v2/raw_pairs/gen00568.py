from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.category") == "Sports") | (F.col("i.store_id") == 4))
    .select(
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
)
