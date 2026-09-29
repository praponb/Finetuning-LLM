from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .select(
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("products.category"),
        F.sort_array(F.col("products.tags")).alias("sorted_tags"),
    )
)
