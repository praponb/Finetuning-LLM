from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.supplier_id") == 38)
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("p.brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.sort_array(F.collect_set(F.col("p.category"))).alias("category_list"),
    )
    .orderBy(F.col("umbrella_count").desc(), "rating_bucket")
)
