from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("products.price") > 330)
    .select(
        F.col("products.launch_date"),
        F.col("products.product_name"),
        F.col("suppliers.rating"),
    )
)
