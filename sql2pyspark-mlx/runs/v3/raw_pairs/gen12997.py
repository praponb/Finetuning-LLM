from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.size(F.col("p.tags")) > 1)
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.brand")))).alias("brand_values"),
    )
    .orderBy("rating_bucket")
)
