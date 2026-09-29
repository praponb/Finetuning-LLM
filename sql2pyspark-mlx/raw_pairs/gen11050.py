from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("ss.revenue").isNotNull())
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("rating_bucket", F.col("cnt").desc())
)
