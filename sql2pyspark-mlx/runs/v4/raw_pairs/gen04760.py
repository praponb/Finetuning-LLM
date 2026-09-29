from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_last())
w2 = Window.orderBy(F.col("rating").desc())
w_grp = Window.partitionBy("country")
w_ordered = Window.partitionBy("country").orderBy(F.col("country_length").desc())
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country", "city")

cleaned = (
    df_suppliers
    .filter(F.col("rating").between(2.1, 2.5))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("city").alias("city_length"),
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["London", "Berlin", "New York"]))
        .select("id", "city")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.sum("rating").over(w_grp).alias("country_sum_rating"),
    )
)

base = (
    scored
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        (F.col("country_sum_rating") + F.col("rating_cume_dist")).alias("country_sum_rating_plus_rating_cume_dist"),
    )
)

remaining_keys_2 = (
    base
    .select("city", "id")
    .subtract(
        df_suppliers
        .filter(F.col("city") != "Singapore")
        .select("city", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.round((F.col("rating_dense_rank") / F.sum("rating_dense_rank").over(w_grp)), 4).alias("rating_dense_rank_share"),
        F.cume_dist().over(w_ordered).alias("country_length_cume_dist"),
    )
)

scored_suppliers = (
    with_windows
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
        F.max("city_length").over(w6).alias("country_max_city_length"),
    )
)

result = (
    scored_suppliers
    .cube("city")
    .agg(
        F.percentile_approx("rating_dense_rank", [0.25, 0.5, 0.75]).alias("rating_dense_rank_quartiles"),
        F.round(F.sum("rating_dense_rank_share") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("rating_dense_rank_share_per_chiang_mai"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
