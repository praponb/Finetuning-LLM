from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.6) | ~F.col("supplier_name").like("Si%"))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("country").desc(), "rating_bucket")
    .limit(5)
)
