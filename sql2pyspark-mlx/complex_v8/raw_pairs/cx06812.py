from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("release_date").asc_nulls_last())
w_artist_id_artist_genre_by_re = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("release_date").desc(), "id")
w_release_date_month_artist_is = Window.partitionBy(F.month("release_date"), "artist_is_verified_2")
w_artist_id_artist_genre_by_re_2 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)

scoped = (
    df_tracks
    .filter((F.col("bpm") > 76) & (F.year(F.col("release_date")) == 2024))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")), "left_anti")
)

joined_artist = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

sequenced = (
    joined_artist
    .withColumn("release_date_cume_dist", F.round(F.cume_dist().over(w_artist_id_artist_is_verified), 3))
)

enriched_artist = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        "s.artist_name",
        "s.artist_is_verified",
        "s.release_date_cume_dist",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

sequenced_2 = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        "artist_name",
        "release_date_cume_dist",
        "artist_debut_year",
        F.lag("artist_debut_year", 1, 0).over(w_artist_id_artist_genre_by_re).alias("prev_artist_debut_year"),
        F.sum("release_date_cume_dist").over(w_release_date_month_artist_is).alias("release_date_month_artist_is_verified_2_sum_rele"),
        F.round(F.col("release_date_cume_dist") - F.avg("release_date_cume_dist").over(w_release_date_month_artist_is), 2).alias("release_date_cume_dist_vs_release_date_month_art"),
        F.row_number().over(w_artist_id_artist_genre_by_re).alias("seq_no"),
        F.max("artist_debut_year").over(w_artist_id_artist_genre_by_re_2).alias("artist_debut_year_max_1_prior"),
    )
)

result = (
    sequenced_2
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        "artist_name",
        "release_date_cume_dist",
        "artist_debut_year",
        "prev_artist_debut_year",
        "release_date_month_artist_is_verified_2_sum_rele",
        "release_date_cume_dist_vs_release_date_month_art",
        "seq_no",
        "artist_debut_year_max_1_prior",
    )
)
