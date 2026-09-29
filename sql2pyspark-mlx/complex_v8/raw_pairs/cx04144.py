from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id", "source").orderBy(F.col("track_release_date_2").desc(), F.col("id").desc())
w_listener_country_by_track_re = Window.partitionBy("listener_country").orderBy(F.col("track_release_date").asc_nulls_last(), "id")
w_track_id_listener_id_by_trac = Window.partitionBy("track_id", "listener_id").orderBy("track_release_date_2", "id")

valid = (
    df_plays
    .filter(F.col("ms_played") >= 51883)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

enriched_track = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.played_at_label",
        F.col("t.release_date").alias("track_release_date"),
    )
)

with_details_track = (
    enriched_track.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.played_at_label",
        "s.track_release_date",
        F.col("t.release_date").alias("track_release_date_2"),
        F.col("t.moods").alias("track_moods"),
        F.col("l.country").alias("listener_country"),
    )
)

latest = (
    with_details_track
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "played_at_label",
        "track_release_date",
        "track_release_date_2",
        "track_moods",
        "listener_country",
        F.ntile(10).over(w_listener_country_by_track_re).alias("track_release_date_decile"),
        F.lead("track_release_date_2", 3).over(w_track_id_listener_id_by_trac).alias("next3_track_release_date_2"),
        F.lag("track_release_date_2").over(w_track_id_listener_id_by_trac).alias("prev_track_release_date_2"),
    )
)

with_details_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.played_at_label",
        "s.track_release_date",
        "s.track_moods",
        "s.listener_country",
        "s.track_release_date_decile",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

detail_rows_plays = (
    with_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") < 290754), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.track_moods",
        "s.listener_country",
        "s.listener_username",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows_plays
    .select(
        "id",
        "track_id",
        "source",
        "track_moods",
        "listener_country",
        "play_id",
        "play_source",
        "play_ms_played",
    )
    .orderBy(F.col("id").desc())
)
