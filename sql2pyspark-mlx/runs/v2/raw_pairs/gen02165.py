from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~(F.col("s.rating").between(2.6, 2.7) & (F.size(F.col("p.tags")) > 2)))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
