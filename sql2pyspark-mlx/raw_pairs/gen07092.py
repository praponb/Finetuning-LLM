from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.city"),
        F.ceil(F.col("i.quantity")).alias("quantity_whole"),
    )
    .orderBy(F.col("quantity_whole").desc())
)
