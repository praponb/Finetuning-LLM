from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("store_sale_count"),
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
    )
    .orderBy("latest_sale_date", "city", "rating_bucket")
    .limit(5)
)
