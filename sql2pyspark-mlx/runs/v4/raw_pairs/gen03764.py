from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .select(F.col("p.brand"), F.col("ssbp.min_units"))
    .orderBy("brand")
)
