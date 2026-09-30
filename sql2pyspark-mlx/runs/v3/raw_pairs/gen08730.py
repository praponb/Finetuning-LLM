from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "outer")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.price"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
    .orderBy(F.col("id").desc())
)
