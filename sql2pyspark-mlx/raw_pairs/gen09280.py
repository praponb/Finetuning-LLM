from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "outer")
    .select(
        F.col("store_sales.product_id"),
        F.col("products.stock_qty"),
        F.col("products.price"),
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
    )
)
