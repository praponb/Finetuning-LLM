from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("rating_tier")
)
