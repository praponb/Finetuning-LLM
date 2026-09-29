from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("rating") > 4.1)
    .select(
        "*",
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)

kept_keys = (
    filtered
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.upper("supplier_name").like("%OKY%"))
        .select("country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered_suppliers = (
    verified
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "supplier_name_country_key",
        F.concat("city", F.lit(" - "), "country").alias("city_country"),
    )
)

result = (
    filtered_suppliers
    .select("id", "country", "city", "supplier_name_country_key", "city_country")
    .orderBy(F.col("id").desc_nulls_first())
)
