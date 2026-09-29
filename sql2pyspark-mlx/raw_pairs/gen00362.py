from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating_cume_dist").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & F.col("supplier_name").like("%Wholesale%"))
    .withColumn("city_group", F.when(F.col("city").isin(["Chiang Mai", "New York"]), "Tier 1").otherwise("Other"))
)

scored = (
    base
    .select(
        "id",
        "country",
        "city",
        "rating",
        "city_group",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.cume_dist().over(w2).alias("rating_cume_dist_2"),
        F.avg("rating").over(w3).alias("rating_avg_3"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "city", "rating_cume_dist_2", "rating_avg_3")
)

remaining_keys = (
    ranked
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["Singapore", "New York", "Bangkok"]))
        .select("city")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("country")
    .agg(
        F.max(F.when(F.col("country") == "JP", F.col("rating_avg_3"))).alias("jp_max_rating_avg_3"),
        F.percentile_approx("rating_avg_3", 0.5).alias("rating_avg_3_p50"),
        F.round(F.avg("rating_avg_3"), 2).alias("avg_rating_avg_3"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.round(F.sum("rating_avg_3") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("rating_avg_3_per_tokyo"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
