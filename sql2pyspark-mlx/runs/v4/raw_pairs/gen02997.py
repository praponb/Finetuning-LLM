from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.count("*").alias("store_sale_count"),
    )
    .filter(F.col("avg_revenue") >= 33)
    .orderBy("brand")
)
