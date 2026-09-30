from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_by_least = Window.partitionBy("artist_is_verified").orderBy(F.col("least_bpm_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_least_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("least_bpm_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id")

prepared = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "party")) & (F.upper(F.trim(F.col("title"))).like("M%")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 1) & (F.upper(F.trim(F.col("t.title"))).like("M%")), "left_semi")
)

joined_artist = (
    qualified.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.least_bpm_duration_sec",
        "s.title_slug",
        "s.release_date_label",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_windows = (
    joined_artist
    .select(
        "*",
        F.nth_value("artist_debut_year", 2).over(w_artist_is_verified_by_least).alias("second_artist_debut_year"),
        F.last("bpm").over(w_artist_country_artist_id_by).alias("last_bpm"),
        F.first("least_bpm_duration_sec", ignorenulls=True).over(w_artist_is_verified_by_least_2).alias("first_known_least_bpm_duration_sec"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "is_explicit",
        "moods",
        "release_date_label",
        "artist_country",
        "artist_debut_year",
        "last_bpm",
        "first_known_least_bpm_duration_sec",
        F.dense_rank().over(w_artist_id_by_artist_debut_ye).alias("artist_debut_year_dense_rank"),
        F.size(F.collect_set("artist_country").over(w_artist_id_artist_is_verified)).alias("distinct_artist_country_in_artist_id_artist_is_v"),
        F.lead("artist_debut_year").over(w_artist_id_by_artist_debut_ye_2).alias("next_artist_debut_year"),
    )
)

exploded_detail_plays = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_country",
        "s.last_bpm",
        "s.artist_debut_year_dense_rank",
        "s.distinct_artist_country_in_artist_id_artist_is_v",
        "s.next_artist_debut_year",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

filtered = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()) & (F.col("p.device") == "phone"), "left_anti")
    .filter(~F.col("play_is_skipped"))
)

result = (
    filtered
    .select(
        "id",
        "artist_debut_year_dense_rank",
        "distinct_artist_country_in_artist_id_artist_is_v",
        "play_ms_played",
        "play_device",
    )
    .orderBy(F.col("artist_debut_year_dense_rank").asc(), "play_device")
)
