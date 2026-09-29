from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.supplier_name").like("G%"))
    .select(
        F.col("products.brand"),
        F.col("products.supplier_id"),
        F.col("products.price"),
        F.col("suppliers.country"),
    )
    .orderBy(F.col("price").desc(), F.col("country").desc())
)
