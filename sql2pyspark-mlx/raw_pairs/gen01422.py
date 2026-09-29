from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "supplier_name_filled",
        F.concat_ws("-", "city", "country").alias("city_country_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

kept_keys = (
    cleaned
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["New York", "Singapore", "Chiang Mai"]))
        .select("country", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("supplier_name").like("Eur%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_country_key",
        "resolved_city",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.avg("rating").over(w2).alias("city_avg_rating"),
    )
)

result = (
    enriched
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.6, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["rating_tier", "country"], ["rating_tier"], []],
        "rating_tier",
        "country",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("country")).alias("num_countries_2"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
    .filter(F.col("jp_rows") > 1)
    .orderBy(F.col("num_countries_2").desc())
)
