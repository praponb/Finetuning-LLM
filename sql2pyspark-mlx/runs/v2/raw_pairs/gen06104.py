from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "outer")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("products.supplier_id") == 29)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("products.product_name"),
        F.col("suppliers.country"),
    )
)
