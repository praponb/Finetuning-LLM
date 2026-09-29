from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
)
