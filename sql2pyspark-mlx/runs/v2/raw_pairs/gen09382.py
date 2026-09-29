from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.revenue").isNotNull())
    .groupBy(F.col("p.supplier_id"), F.col("p.brand"))
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
    .orderBy(F.col("revenue_count").desc())
)
