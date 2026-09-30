from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
    )
    .filter(F.col("rating_p95") < 1000000)
    .orderBy(F.col("rating_p95").asc_nulls_first())
)
