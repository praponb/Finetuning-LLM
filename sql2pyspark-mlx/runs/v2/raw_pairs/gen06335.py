from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
    )
)
