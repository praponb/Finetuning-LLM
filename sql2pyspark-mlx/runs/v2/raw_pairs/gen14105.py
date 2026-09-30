from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(~F.col("suppliers.supplier_name").like("P%"))
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.price"),
        F.col("products.launch_date"),
        F.col("suppliers.rating"),
    )
)
