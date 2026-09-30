from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country")

remaining_keys = (
    df_suppliers
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.upper("supplier_name").like("%ACI%"))
        .select("city", "country")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "new")), "left_semi")
)

scored = (
    not_flagged
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("city_sum_rating", F.sum("rating").over(w))
    .withColumn("city_count_rating", F.count("rating").over(w))
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("city_sum_rating") / F.sum("city_sum_rating").over(w)), 4).alias("city_sum_rating_share"),
        F.avg("city_count_rating").over(w2).alias("country_avg_city_count_rating"),
    )
)

base = (
    with_windows
    .filter(F.col("city_count_rating") == 6)
    .withColumn("supplier_name_city", F.concat("supplier_name", F.lit(" "), "city"))
)

result = (
    base
    .groupBy(
        F.when(F.col("city_sum_rating") >= 20, "HIGH").otherwise("LOW").alias("city_sum_rating_tier"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.round(F.avg("country_avg_city_count_rating"), 2).alias("avg_country_avg_city_count_rating"),
        F.percentile_approx("country_avg_city_count_rating", 0.9).alias("country_avg_city_count_rating_p90"),
        F.avg(F.when(F.col("country") == "US", F.col("city_count_rating"))).alias("us_avg_city_count_rating"),
    )
    .orderBy(F.col("th_count").desc_nulls_first())
)
