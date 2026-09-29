from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("price").isNotNull()).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .filter(F.col("suppliers.rating") > 2.9)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
    .orderBy(F.col("rating").desc())
)
