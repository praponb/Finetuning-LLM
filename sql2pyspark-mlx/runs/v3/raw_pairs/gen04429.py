from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.length(F.col("p.product_name")) < 14)
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
    )
)
