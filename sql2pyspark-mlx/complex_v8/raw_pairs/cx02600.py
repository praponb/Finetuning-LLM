from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_is_skipped_2_artist_cou = Window.partitionBy("play_is_skipped_2", "artist_country_2").orderBy("artist_debut_year", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_is_skipped_2_artist_cou_2 = Window.partitionBy("play_is_skipped_2", "artist_country_2").orderBy("artist_debut_year", "id", "play_id")
w_play_id = Window.partitionBy("play_id")

filtered = (
    df_tracks
    .filter(F.col("bpm") > 82)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

with_parent_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

exploded_detail_plays = (
    with_parent_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") < 223828), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.artist_country",
        "s.artist_debut_year",
        "s.artist_name",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_parent = (
    exploded_detail_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.artist_country",
        "s.artist_debut_year",
        "s.artist_name",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_device",
        "s.play_ms_played",
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.country").alias("artist_country_2"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.last("play_ms_played", ignorenulls=True).over(w_play_is_skipped_2_artist_cou).alias("last_known_play_ms_played"),
        F.ntile(5).over(w_play_is_skipped_2_artist_cou_2).alias("artist_debut_year_quintile"),
        F.sum("play_ms_played").over(w_play_id).alias("play_id_sum_play_ms_played"),
    )
)

result = (
    windowed
    .select(
        "id",
        "play_id",
        "play_ms_played",
        "artist_debut_year",
        "last_known_play_ms_played",
        "artist_debut_year_quintile",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played", "artist_debut_year", "last_known_play_ms_played", "artist_debut_year_quintile"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "play_id", "metric")
)
