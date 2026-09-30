from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Global%") & F.col("supplier_name").like("%Globa%"))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("supplier_count"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("rating_bucket", F.col("supplier_count").desc())
    .limit(1)
)
