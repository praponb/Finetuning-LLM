from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_by_artist_deb = Window.partitionBy("artist_country").orderBy("artist_debut_year", "id")
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_id_by_bpm_distance = Window.partitionBy("artist_id").orderBy("bpm_distance", "id")
w_artist_country_by_artist_deb_2 = Window.partitionBy("artist_country").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.abs(F.col("bpm") - 142).alias("bpm_distance"),
    )
)

combined_details_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.length(F.trim(F.col("a.artist_name"))) >= 7)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.bpm_distance",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

ranked = (
    combined_details_artist
    .select(
        "id",
        "is_explicit",
        "bpm",
        "bpm_distance",
        "artist_country",
        "artist_name",
        F.lead("bpm", 2).over(w_artist_country_by_artist_deb).alias("next2_bpm"),
        F.count(F.lit(1)).over(w_artist_id_artist_country).alias("rows_in_artist_id_artist_country"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_bpm_distance)).alias("duration_sec_change"),
        F.nth_value("artist_debut_year", 2).over(w_artist_country_by_artist_deb_2).alias("second_artist_debut_year"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix"),
        F.concat_ws(": ", F.col("artist_name"), F.col("artist_country")).alias("artist_name_artist_country_label"),
    )
)

annotated = (
    tagged
    .select(
        "id",
        "bpm",
        "artist_country",
        "artist_name",
        "duration_sec_change",
        "artist_name_prefix",
        (F.floor(F.col("bpm_distance") / 25) * 25).alias("bpm_distance_bucket"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isNotNull()) & (F.col("p.played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_anti")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("duration_sec_change").alias("overall_avg_duration_sec_change")))
    .filter(F.col("duration_sec_change") > F.col("overall_avg_duration_sec_change"))
    .drop("overall_avg_duration_sec_change")
)

result = (
    qualified
    .select(
        "id",
        "bpm",
        "artist_country",
        "artist_name",
        "duration_sec_change",
        "artist_name_prefix",
        "bpm_distance_bucket",
        "is_explicit_label",
    )
    .orderBy("artist_name")
)
