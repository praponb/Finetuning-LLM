from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ssbp.max_revenue"),
        F.col("ssbp.units_count"),
    )
    .orderBy("stock_qty")
)
