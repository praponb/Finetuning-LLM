from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.cost"),
        F.col("t.total_revenue"),
        F.col("t.first_sale_date"),
    )
)
