from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .select(
        "*",
        F.round((F.col("rating") * 1.07), 2).alias("adjusted_rating"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isin(["Acme", "Stark"])), "left_anti")
)

valid_suppliers = (
    kept
    .filter(F.col("city").isNotNull() & (F.col("adjusted_rating") <= 50))
    .select(
        "*",
        F.least("adjusted_rating", "rating").alias("least_adjusted_rating_rating"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city_2"),
    )
)

kept_suppliers = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Umbrella"), "left_anti")
)

result = (
    kept_suppliers
    .rollup(
        "country",
        F.when(F.col("rating") >= 3.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("least_adjusted_rating_rating"), 2).alias("avg_least_adjusted_rating_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
