from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .orderBy("avg_revenue", "id", "product_name")
    .limit(10)
)
