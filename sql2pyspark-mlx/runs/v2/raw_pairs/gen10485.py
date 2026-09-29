from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("i.product_id").isin([5, 7, 9]) | (F.col("s.country") == "US"))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
