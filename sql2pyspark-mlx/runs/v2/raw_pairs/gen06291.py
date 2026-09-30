from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "bestseller") | F.col("products.brand").isin(["Globex", "Initech"]))
    .select(
        F.col("products.product_name"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("suppliers.supplier_name"),
    )
    .orderBy("country")
)
