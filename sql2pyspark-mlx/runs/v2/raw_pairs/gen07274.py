from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.supplier_name").like("Eu%"))
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.supplier_id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
    )
    .orderBy(F.col("id").desc(), "launch_date")
)
