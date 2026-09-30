from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("ss.product_id").isin([3, 5, 10]))
    .groupBy(
        F.when(F.col("s.rating") >= 3.9, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 1)
)
