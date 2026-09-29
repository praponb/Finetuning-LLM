from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

scoped_tracks = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 4) | (F.col("duration_sec") >= 142))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        F.date_add(F.col("release_date"), 90).alias("release_date_plus_90d"),
    )
)

latest_per_key = (
    scoped_tracks
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_artist = (
    latest_per_key.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.release_date_plus_90d",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

artist_id_stats = (
    with_details_artist
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

outliers = (
    with_details_artist.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

combined_details_artist = (
    outliers.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.artist_name").alias("artist_artist_name"),
    )
)

rollup_base = (
    combined_details_artist
    .groupBy("artist_is_verified", "artist_genre")
    .agg(
        F.sum("max_bpm").alias("total_max_bpm"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("bpm_minus_max"), 2).alias("avg_bpm_minus_max"),
    )
    .filter(F.col("n_rows") >= 3)
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("avg_bpm_minus_max").alias("overall_avg_avg_bpm_minus_max")))
    .filter(F.col("avg_bpm_minus_max") > F.col("overall_avg_avg_bpm_minus_max"))
    .drop("overall_avg_avg_bpm_minus_max")
)

result = (
    without_matches
    .select(
        "artist_is_verified",
        "artist_genre",
        "n_rows",
        "avg_bpm_minus_max",
    )
    .orderBy(F.col("avg_bpm_minus_max").asc_nulls_last())
)
