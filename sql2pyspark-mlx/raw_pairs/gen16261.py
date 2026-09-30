from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

kept_keys = (
    cleaned
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("id", "city")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select("id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.sum("rating").over(w).alias("country_sum_rating"),
        F.min("rating").over(w2).alias("city_min_rating"),
    )
)

valid = (
    with_windows
    .filter(F.col("rating").between(2.1, 4.0))
    .select(
        "id",
        "supplier_name",
        "country",
        "country_sum_rating",
        "city_min_rating",
        F.concat("supplier_name", F.lit("/"), "country").alias("supplier_name_country"),
    )
)

valid_suppliers = (
    valid
    .filter(F.col("city_min_rating").isNotNull())
    .select(
        "country",
        "city_min_rating",
        "supplier_name_country",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

result = (
    valid_suppliers
    .select("country", "city_min_rating")
    .orderBy(F.col("country").desc(), F.col("city_min_rating").desc())
)
