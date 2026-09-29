from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .filter(F.col("products.supplier_id") == 22)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id"),
        F.col("products.stock_qty"),
        F.col("products.price"),
        F.col("suppliers.country"),
    )
)
