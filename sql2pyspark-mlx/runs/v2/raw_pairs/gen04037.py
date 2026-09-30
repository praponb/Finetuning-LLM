from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("revenue") >= 3300)
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .filter(F.col("ssbp.avg_units") > 36)
    .select(
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("ssbp.unique_products"),
        F.col("ssbp.avg_units"),
    )
)
