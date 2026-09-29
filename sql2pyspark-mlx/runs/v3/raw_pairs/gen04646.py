from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("products.category") == "Electronics")
    .groupBy(F.col("products.supplier_id"))
    .agg(
        F.countDistinct(F.col("store_sales.revenue")).alias("unique_products"),
        F.max(F.col("store_sales.revenue")).alias("max_revenue"),
    )
    .orderBy(F.col("max_revenue").desc(), "supplier_id")
    .limit(5)
)
