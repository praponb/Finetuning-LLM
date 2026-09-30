from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_store_sales"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.total_revenue") > 9000)
    .select(
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("ps.num_store_sales"),
        F.col("ps.total_revenue"),
    )
)
