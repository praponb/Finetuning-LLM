from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.supplier_name").like("%Toky%") & ~F.col("p.brand").isin(["Stark", "Umbrella"]))
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.rating"),
    )
)
