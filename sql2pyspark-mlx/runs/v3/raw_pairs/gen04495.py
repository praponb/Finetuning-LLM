from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-02-01")
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .filter(F.col("ssbp.max_units") > 28)
    .select(
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("ssbp.revenue_count"),
        F.col("ssbp.max_units"),
    )
    .orderBy("launch_date")
)
