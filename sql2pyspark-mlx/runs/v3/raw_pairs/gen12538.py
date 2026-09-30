from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id") == 5)
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
    .orderBy(F.col("category").desc(), "brand")
    .limit(5)
)
