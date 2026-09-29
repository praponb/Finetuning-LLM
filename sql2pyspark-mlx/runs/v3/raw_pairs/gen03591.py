from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("suppliers.city").isNull())
    .select(
        F.col("store_sales.product_id"),
        F.col("products.product_name"),
        F.col("products.cost"),
        F.col("suppliers.city"),
    )
)
