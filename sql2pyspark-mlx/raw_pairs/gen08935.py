from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") <= 3.0)
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") > 2.7)
        .select("country", "id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Electronics", "Books", "Fashion"])), "left_semi")
)

cleaned = (
    eligible
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "city_upper",
        "resolved_city",
        F.concat_ws("/", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)

result = cleaned.select("city", "resolved_city", "country_supplier_name_key")
