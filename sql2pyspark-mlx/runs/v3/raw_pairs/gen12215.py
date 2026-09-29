from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("ssbp.unique_stores"),
        F.col("ssbp.avg_revenue"),
    )
    .orderBy(F.col("supplier_id").desc(), "id")
    .limit(5)
)
