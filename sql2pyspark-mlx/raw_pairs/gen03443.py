from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 3200))
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .filter(F.col("ssbp.min_units") >= 15)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ssbp.min_units"))
)
