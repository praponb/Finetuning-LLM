from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .filter(F.col("ssbp.store_sale_count") > 7)
    .select(
        F.col("p.brand"),
        F.col("ssbp.unique_stores"),
        F.col("ssbp.store_sale_count"),
    )
    .orderBy("brand", F.col("store_sale_count").desc())
)
