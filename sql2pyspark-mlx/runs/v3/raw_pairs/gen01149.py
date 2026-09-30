from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("products.product_name"),
        F.add_months(F.col("products.launch_date"), 12).alias("launch_date_plus_months"),
    )
)
