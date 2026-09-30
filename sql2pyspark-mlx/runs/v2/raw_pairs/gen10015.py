from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("product_count"),
        F.sum(F.when(F.col("p.category") == "Fashion", F.col("p.stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
    )
    .filter(F.col("product_count") >= 2)
    .orderBy(F.col("product_count").desc(), "rating_bucket")
)
