from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("s.supplier_name").like("%Glo%") | (F.col("s.country") != "SG"))
    .select(
        F.col("s.rating"),
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.brand"),
        F.upper(F.col("p.product_name")).alias("product_name_upper"),
    )
)
