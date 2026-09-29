from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")

cleaned_plays = (
    df_plays
    .filter((F.col("is_skipped")) | (F.col("source").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

sequenced = (
    cleaned_plays
    .select(
        "*",
        F.sum("ms_played").over(w_is_skipped_track_id).alias("is_skipped_track_id_sum_ms_played"),
        F.lag("ms_played", 2).over(w_source_by_ms_played).alias("prev2_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_by_ms_played)).alias("ms_played_change"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

labeled = (
    with_activity
    .filter(F.col("listener_id") <= 9)
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_change",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_parent_track = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_change",
        "s.is_skipped_label",
        "s.device_slug",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_parent_track
    .select(
        "id",
        "is_skipped",
        "source",
        "ms_played_change",
        "is_skipped_label",
        "device_slug",
        "track_artist_id",
        "artist_genre",
        F.greatest(F.col("ms_played_change"), F.col("ms_played")).alias("greatest_ms_played_change_ms_played"),
        F.lower(F.trim(F.col("device_slug"))).alias("device_slug_clean"),
    )
    .orderBy(
        F.col("greatest_ms_played_change_ms_played").asc(),
        F.col("device_slug").desc(),
    )
)
