from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_bpm_moving_1 = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(-1, Window.currentRow)
w_all_by_bpm_prior_3 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, -1)

prepared = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("bpm") >= 76))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.sum("bpm").over(w_artist_id_by_bpm_running).alias("running_bpm"),
        F.row_number().over(w_all_by_bpm).alias("rn"),
        F.max("bpm").over(w_is_explicit).alias("is_explicit_max_bpm"),
        F.max("bpm").over(w_artist_id_by_bpm_moving_1).alias("bpm_max_2"),
        F.max("bpm").over(w_artist_id_by_bpm_running).alias("max_bpm_so_far"),
        F.round(F.avg("bpm").over(w_all_by_bpm_prior_3), 2).alias("bpm_avg_3_prior"),
    )
)

enriched_artist = (
    with_trends.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.running_bpm",
        "s.rn",
        "s.is_explicit_max_bpm",
        "s.bpm_max_2",
        "s.max_bpm_so_far",
        "s.bpm_avg_3_prior",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_parent_artist = (
    enriched_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.running_bpm",
        "s.rn",
        "s.is_explicit_max_bpm",
        "s.bpm_max_2",
        "s.max_bpm_so_far",
        "s.bpm_avg_3_prior",
        "s.artist_is_verified",
        "s.artist_country",
        "s.artist_debut_year",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.country").alias("artist_country_2"),
    )
)

result = (
    with_parent_artist
    .groupingSets(
        [["artist_is_verified_2", "artist_country"], ["artist_is_verified_2"]],
        "artist_is_verified_2", "artist_country",
    )
    .agg(
        F.round(F.sum("running_bpm") / F.nullif(F.sum("artist_debut_year"), F.lit(0)), 3).alias("running_bpm_to_artist_debut_year_ratio"),
    )
    .select(
        "artist_is_verified_2",
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "running_bpm_to_artist_debut_year_ratio",
    )
    .orderBy(
        F.col("artist_is_verified_2").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
