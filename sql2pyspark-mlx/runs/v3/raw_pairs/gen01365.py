from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .select(
        F.col("store_sales.revenue"),
        F.col("products.product_name"),
        F.col("suppliers.rating"),
        F.floor(F.col("products.price")).alias("price_whole"),
    )
    .orderBy("product_name", "price_whole")
)
