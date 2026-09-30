from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating") > 2.5)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") != 263), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.length("supplier_name") > 7)
    .select(
        "id",
        "country",
        "supplier_name_country_key",
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

kept_keys = (
    filtered
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("id", "country")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup("country")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count("*").alias("cnt"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("country") == "DE").alias("de_events_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
