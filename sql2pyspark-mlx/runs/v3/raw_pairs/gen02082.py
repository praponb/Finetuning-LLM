from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.col("products.launch_date"),
    )
)
