from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-05-01", "2024-05-01"))
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .select(
        F.col("p.launch_date"),
        F.col("ssbp.min_sale_date"),
        F.col("ssbp.store_sale_count"),
    )
    .orderBy("min_sale_date")
)
