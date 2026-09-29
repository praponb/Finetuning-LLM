from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("products.category"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
    .orderBy(F.col("rating").desc())
)
