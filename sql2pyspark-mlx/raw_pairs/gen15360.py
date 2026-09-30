from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("units") > 37)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.brand"),
        F.col("t.store_sale_count"),
        F.col("t.units_range"),
    )
)
