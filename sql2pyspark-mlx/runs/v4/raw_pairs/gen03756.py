from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([2, 6, 7, 10]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("s.rating"),
        F.col("s.city"),
    )
)
