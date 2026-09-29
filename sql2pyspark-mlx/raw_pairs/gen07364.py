from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("i.last_restock") >= "2024-06-19")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)
