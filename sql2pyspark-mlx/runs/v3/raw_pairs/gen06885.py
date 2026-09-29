from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3]))
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("store_sales_by_product"), F.col("products.id") == F.col("store_sales_by_product.product_id"), "left")
    .filter(F.col("store_sales_by_product.max_units") > 18)
    .select(
        F.col("products.launch_date"),
        F.col("store_sales_by_product.min_units"),
        F.col("store_sales_by_product.max_units"),
    )
    .orderBy("min_units")
)
