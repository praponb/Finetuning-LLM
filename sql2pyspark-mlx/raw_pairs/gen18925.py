from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.col("rating") <= 1.8)
        .select("city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 630), "left_semi")
)

valid = (
    kept
    .filter(F.col("city").isNotNull() & (F.lower(F.trim("supplier_name")) == "pacific wholesale"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

matched_keys = (
    valid
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("city") == "Bangkok")
        .select("country", "id")
    )
)

reconciled_suppliers = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_suppliers = (
    reconciled_suppliers
    .filter((F.col("city").eqNullSafe(F.lit("New York"))) & (F.col("rating_filled") < 54))
    .select(
        "country",
        "rating",
        "rating_filled",
        "city_filled",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

result = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
