from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.cost") > 660) & ~F.col("s.country").isin(["JP", "TH"]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.city"),
    )
)
