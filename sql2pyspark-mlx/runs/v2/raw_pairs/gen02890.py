from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.month("sale_date").isin([7, 9, 10]))
    .groupBy("product_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .select(F.col("p.cost"), F.col("p.launch_date"), F.col("ssbp.revenue_count"))
    .orderBy(F.col("launch_date").desc())
)
