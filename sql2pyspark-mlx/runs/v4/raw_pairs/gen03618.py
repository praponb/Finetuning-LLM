from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category").isin(["Sports", "Home"]) | ~F.col("s.city").isin(["Tokyo", "New York", "Berlin"]))
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("p.brand"),
        F.concat(F.col("p.category"), F.lit(" "), F.col("p.product_name")).alias("category_product_name"),
    )
)
