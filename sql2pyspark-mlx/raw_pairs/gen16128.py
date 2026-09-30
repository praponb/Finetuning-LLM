from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("revenue") <= 3100)
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ssbp.min_revenue"),
        F.col("ssbp.max_revenue"),
    )
    .orderBy(F.col("min_revenue").desc(), "stock_qty")
)
