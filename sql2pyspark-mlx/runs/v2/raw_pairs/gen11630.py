from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "UK")
    .select(
        F.col("i.store_id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy(F.col("country").desc())
)
