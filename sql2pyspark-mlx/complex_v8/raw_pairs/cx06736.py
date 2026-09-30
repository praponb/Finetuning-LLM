from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_device = Window.partitionBy("play_device")
w_artist_id_play_listener_id_b = Window.partitionBy("artist_id", "play_listener_id").orderBy("duration_sec_distance", "id", "play_id").rowsBetween(-6, Window.currentRow)
w_artist_id_play_listener_id_b_2 = Window.partitionBy("artist_id", "play_listener_id").orderBy("duration_sec_distance", "id", "play_id")

filtered = (
    df_tracks
    .filter((F.col("duration_sec") >= 184) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
    )
)

line_level = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.duration_sec_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

filtered_2 = (
    line_level.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")), "left_semi")
)

with_windows = (
    filtered_2
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "duration_sec_distance",
        "play_id",
        "play_device",
        "play_listener_id",
        F.round(F.col("duration_sec_distance") / F.nullif(F.sum("duration_sec_distance").over(w_play_device), F.lit(0)), 4).alias("duration_sec_distance_share_of_play_device"),
        F.max("duration_sec").over(w_artist_id_play_listener_id_b).alias("duration_sec_max_7"),
        F.lead("release_date").over(w_artist_id_play_listener_id_b_2).alias("next_release_date"),
        F.sum("duration_sec").over(w_play_device).alias("play_device_sum_duration_sec"),
    )
)

with_details_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .filter(F.col("l.signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-10-18").cast("date")))
    .select(
        "s.id",
        "s.title",
        "s.release_date",
        "s.duration_sec_distance",
        "s.play_id",
        "s.play_device",
        "s.play_listener_id",
        "s.duration_sec_distance_share_of_play_device",
        "s.duration_sec_max_7",
        "s.next_release_date",
        "s.play_device_sum_duration_sec",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    with_details_listener
    .select(
        "id",
        "play_id",
        "duration_sec_distance",
        "listener_birth_year",
        "play_device_sum_duration_sec",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["duration_sec_distance", "listener_birth_year", "play_device_sum_duration_sec"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
