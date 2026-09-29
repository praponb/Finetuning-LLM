from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter((F.col("store_sales.revenue") > 2200) & F.col("suppliers.country").isin(["US", "DE", "JP"]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("products.product_name"),
        F.col("suppliers.supplier_name"),
    )
)
