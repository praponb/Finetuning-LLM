from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("price") >= 600).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .filter(F.upper(F.col("suppliers.supplier_name")).like("%OKY%"))
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
)
