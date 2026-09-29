from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("ssbp.stddev_units"),
        F.col("ssbp.min_revenue"),
    )
    .orderBy(F.col("min_revenue").desc(), "id")
    .limit(10)
)
