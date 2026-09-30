from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.store_id").isin([4, 5, 6]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .orderBy("brand")
)
