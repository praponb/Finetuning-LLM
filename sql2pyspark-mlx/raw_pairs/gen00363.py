from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("products.brand").isNotNull())
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
        F.array_contains(F.col("products.tags"), "premium").alias("is_premium"),
    )
)
