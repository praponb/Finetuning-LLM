from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("store_sales.revenue").between(1500, 2400))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("products.supplier_id"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
        F.round(F.col("store_sales.units"), 1).alias("units_rounded"),
    )
)
