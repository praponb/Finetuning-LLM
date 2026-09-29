from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city_filled",
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") > 390), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Initech"), "left_semi")
)

grouped = (
    not_flagged
    .groupBy("country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select("country", "th_count")
    .orderBy(F.col("country").desc(), F.col("th_count").desc())
)
