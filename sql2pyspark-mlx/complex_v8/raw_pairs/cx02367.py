from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country").orderBy("min_track_bpm", "id")

filtered = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("country") == "US"))
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

qualified = (
    filtered
    .crossJoin(filtered.agg(F.max("debut_year_distance").alias("overall_max_debut_year_distance")))
    .filter(F.col("debut_year_distance") < F.col("overall_max_debut_year_distance"))
    .drop("overall_max_debut_year_distance")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.sum("duration_sec").alias("total_track_duration_sec"),
        F.min("bpm").alias("min_track_bpm"),
    )
)

with_stats = (
    qualified.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.total_track_duration_sec", "t.min_track_bpm")
)

latest = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupingSets(
        [["country", "is_verified"], ["country"], ["is_verified"]],
        "country", "is_verified",
    )
    .agg(F.array_sort(F.collect_set("country")).alias("country_list"))
    .select(
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        "is_verified",
        "country_list",
    )
    .orderBy(
        F.col("country").asc_nulls_last(),
        F.col("is_verified").asc_nulls_last(),
    )
)
