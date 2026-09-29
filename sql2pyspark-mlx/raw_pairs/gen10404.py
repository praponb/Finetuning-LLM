from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("rating").between(1.9, 4.0))
    .select(
        "*",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
    )
)

matched_keys = (
    valid
    .select("country")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating").between(2.7, 3.9))
        .select("country")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") >= 298), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("city").isNotNull() & F.col("country").isin(["UK", "DE", "TH"]))
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
)

base_suppliers = (
    base
    .filter(F.col("city").isNotNull() & (F.col("rating") <= 2.9))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "supplier_name_normalized",
        "supplier_name_lower",
        F.substring("city", 1, 3).alias("city_prefix"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    base_suppliers
    .rollup("country", "city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.grouping("country").alias("grouping_id"),
    )
    .filter(F.col("distinct_cities") > 1)
    .orderBy(F.col("num_countries").desc())
)
