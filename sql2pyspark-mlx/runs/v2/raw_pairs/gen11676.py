from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_store_sales"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.stock_qty"), F.col("ps.num_store_sales"), F.col("ps.row_count"))
)
