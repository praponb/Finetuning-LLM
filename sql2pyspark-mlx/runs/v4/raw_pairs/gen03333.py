from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("rating_filled") >= 64, "LARGE").otherwise("SMALL").alias("rating_filled_tier"),
    )
    .agg(
        F.round(F.avg("rating_filled"), 2).alias("avg_rating_filled"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("rating").alias("rating_stddev"),
    )
)
