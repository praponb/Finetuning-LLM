from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
    )
    .filter(F.col("avg_revenue") > 62)
    .orderBy("country")
)
