from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_or_unknown_by_debut = Window.partitionBy("country_or_unknown").orderBy("debut_year_distance", "id").rowsBetween(-2, Window.currentRow)
w_country_or_unknown_genre = Window.partitionBy("country_or_unknown", "genre")

prepared = (
    df_artists
    .filter((F.col("country").isin("BR", "JP", "US")) & (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2015).alias("debut_year_distance"),
    )
)

tagged = (
    prepared
    .filter(F.col("debut_year") > 1997)
    .select(
        "*",
        (F.floor(F.col("debut_year_distance") / 500) * 500).alias("debut_year_distance_bucket"),
        F.round(F.col("debut_year_distance") / F.nullif(F.col("debut_year"), F.lit(0)), 3).alias("debut_year_distance_per_debut_year"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.duration_sec").between(164, 392)) & (F.col("t.title").isNotNull()), "left_anti")
    .filter(F.col("debut_year") <= 2016)
)

metrics = (
    with_activity
    .withColumn("debut_year_distance_bucket_avg_3", F.round(F.avg("debut_year_distance_bucket").over(w_country_or_unknown_by_debut), 2))
    .withColumn("distinct_country_in_country_or_unknown_genre", F.size(F.collect_set("country").over(w_country_or_unknown_genre)))
)

totals = (
    metrics
    .groupBy(
        "distinct_country_in_country_or_unknown_genre",
        "country_or_unknown",
        "is_verified",
    )
    .agg(
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.sum("distinct_country_in_country_or_unknown_genre").alias("total_distinct_country_in_country_or_unknown_gen"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country").isNotNull(), F.col("debut_year_distance")).otherwise(0)).alias("debut_year_distance_country_is_not_null"),
    )
)

screened = (
    totals
    .crossJoin(totals.agg(F.max("total_distinct_country_in_country_or_unknown_gen").alias("overall_max_total_distinct_country_in_country_or")))
    .filter(F.col("total_distinct_country_in_country_or_unknown_gen") < F.col("overall_max_total_distinct_country_in_country_or"))
    .drop("overall_max_total_distinct_country_in_country_or")
)

result = (
    screened
    .groupBy("distinct_ids")
    .pivot("country_or_unknown", ["TH", "KR"])
    .agg(F.min("total_distinct_country_in_country_or_unknown_gen"))
)
