from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_moving = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("source").orderBy(F.col("ms_played_avg_2").desc(), F.col("id").desc())

cleaned_plays = df_plays.filter((F.col("device") == "tv") & (F.col("listener_id").isNotNull()))

with_windows = (
    cleaned_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.round(F.avg("ms_played").over(w_track_id_by_ms_played_moving), 2).alias("ms_played_avg_2"),
        F.sum("ms_played").over(w_track_id_by_ms_played_runnin).alias("running_ms_played"),
    )
)

scored = with_windows.withColumn("played_at_date", F.to_date(F.col("played_at")))

joined = (
    scored.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.title").alias("track_title"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

latest_per_key = (
    joined
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched_track = (
    latest_per_key.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.running_ms_played",
        "s.played_at_date",
        "s.track_release_date",
        "s.track_title",
        "s.artist_debut_year",
        "s.artist_is_verified",
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    enriched_track
    .select(
        "id",
        "played_at",
        "source",
        "running_ms_played",
        "track_release_date",
        "track_title",
        "artist_debut_year",
        "artist_is_verified",
    )
)
