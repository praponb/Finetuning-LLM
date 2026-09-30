from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("store_sales_by_product"), F.col("products.id") == F.col("store_sales_by_product.product_id"), "left")
    .filter(F.col("store_sales_by_product.units_count") > 9)
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("store_sales_by_product.units_count"),
    )
)
