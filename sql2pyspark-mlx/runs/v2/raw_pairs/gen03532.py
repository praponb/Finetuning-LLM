from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~(F.col("p.price").between(350, 730) & F.col("s.supplier_name").like("Li%")))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
