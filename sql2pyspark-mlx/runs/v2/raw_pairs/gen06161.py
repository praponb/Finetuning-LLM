from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("price") > 770).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
    )
    .orderBy(F.col("id").desc(), F.col("supplier_name").desc())
)
