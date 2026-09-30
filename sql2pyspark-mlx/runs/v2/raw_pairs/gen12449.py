from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "right")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
    )
    .orderBy("product_name")
)
