from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_moods_count_by_artist_debut = Window.partitionBy("moods_count").orderBy("artist_debut_year", "id")
w_moods_count_by_artist_debut_2 = Window.partitionBy("moods_count").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tracks
    .filter(F.col("duration_sec") <= 392)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.size(F.col("moods")).alias("moods_count"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

enriched_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods_count",
        "s.release_date_week",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "release_date_week",
        "artist_is_verified",
        "artist_debut_year",
        "artist_country",
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.col("moods_count") / F.nullif(F.sum("moods_count").over(w_artist_id), F.lit(0)), 4).alias("moods_count_share_of_artist_id"),
        (F.col("bpm") - F.lag("bpm").over(w_moods_count_by_artist_debut)).alias("bpm_change"),
        F.nth_value("release_date", 2).over(w_moods_count_by_artist_debut_2).alias("second_release_date"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "release_date",
        "release_date_week",
        "artist_is_verified",
        "artist_debut_year",
        "artist_id_max_bpm",
        "moods_count_share_of_artist_id",
        "bpm_change",
        "second_release_date",
        F.date_trunc("quarter", F.col("release_date_week")).alias("release_date_week_quarter"),
        (F.col("bpm_change") + F.col("artist_id_max_bpm")).alias("bpm_change_plus_artist_id_max_bpm"),
    )
)
