from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country").orderBy(F.col("country_sum_rating").desc())

matched_keys = (
    df_suppliers
    .select("city", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("supplier_name").like("%Paci%"))
        .select("city", "id")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("city_stddev_samp_rating", F.stddev_samp("rating").over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("rating_share_2", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("city_sum_rating", F.sum("rating").over(w))
    .withColumn("country_sum_rating", F.sum("rating").over(w2))
)

with_windows = (
    scored
    .filter(F.col("country") == "DE")
    .withColumn("country_sum_rating_rank", F.rank().over(w3))
    .withColumn("city_min_country_sum_rating", F.min("country_sum_rating").over(w))
)

cleaned = (
    with_windows
    .filter(~F.col("supplier_name").like("T%"))
    .select(
        "id",
        "country",
        "rating",
        "city_stddev_samp_rating",
        "rating_share",
        "city_sum_rating",
        "country_sum_rating",
        "city_min_country_sum_rating",
        (F.col("rating") - F.col("city_stddev_samp_rating")).alias("rating_minus_city_stddev_samp_rating"),
        F.col("city_min_country_sum_rating").cast("string").alias("city_min_country_sum_rating_num"),
    )
)

valid = (
    cleaned
    .filter(F.col("rating") <= 2.4)
    .select(
        "*",
        F.round((F.col("country_sum_rating") / F.col("rating")), 2).alias("country_sum_rating_per_rating"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating_minus_city_stddev_samp_rating") >= 75, "LARGE").otherwise("SMALL").alias("rating_minus_city_stddev_samp_rating_tier"),
        "country",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("num_rows"),
        F.max("country_sum_rating_per_rating").alias("max_country_sum_rating_per_rating"),
        F.size(F.collect_set("country")).alias("num_countries_2"),
        F.round(F.avg("rating_minus_city_stddev_samp_rating"), 2).alias("avg_rating_minus_city_stddev_samp_rating"),
    )
)
