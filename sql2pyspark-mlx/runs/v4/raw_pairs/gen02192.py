from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("price").isNotNull()).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_anti")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
)
