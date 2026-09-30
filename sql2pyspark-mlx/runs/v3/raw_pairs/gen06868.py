from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
)
