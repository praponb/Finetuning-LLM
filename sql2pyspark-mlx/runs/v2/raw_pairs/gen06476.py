from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") > "2024-12-24")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.rating"),
        F.add_months(F.col("i.last_restock"), 1).alias("last_restock_plus_months"),
    )
)
