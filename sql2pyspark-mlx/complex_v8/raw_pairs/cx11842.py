from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_device_play_id_by_first = Window.partitionBy("play_device", "play_id").orderBy(F.col("first_duration_sec").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_is_skipped = Window.partitionBy("play_is_skipped")
w_play_device_play_id_by_first_2 = Window.partitionBy("play_device", "play_id").orderBy(F.col("first_duration_sec").desc(), "id", "play_id")

prepared_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter_2"),
    )
)

filtered = (
    prepared_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

sequenced = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("first_known_duration_sec"),
        F.first("duration_sec").over(w_artist_id_by_bpm_full).alias("first_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
    )
)

line_level = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isin("search", "radio", "album")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.duration_sec_cume_dist",
        "s.duration_sec_percent_rank",
        "s.first_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

with_activity = (
    line_level
    .crossJoin(line_level.agg(F.avg("duration_sec").alias("overall_avg_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_avg_duration_sec"))
    .drop("overall_avg_duration_sec")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

sequenced_2 = (
    with_activity_2
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "duration_sec_cume_dist",
        "duration_sec_percent_rank",
        "first_duration_sec",
        "play_id",
        "play_source",
        "play_device",
        "play_listener_id",
        F.min("duration_sec_percent_rank").over(w_play_device_play_id_by_first).alias("min_duration_sec_percent_rank_so_far"),
        F.min("duration_sec").over(w_play_is_skipped).alias("play_is_skipped_min_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_play_is_skipped), F.lit(0)), 4).alias("duration_sec_share_of_play_is_skipped"),
        (F.col("first_duration_sec") - F.lag("first_duration_sec").over(w_play_device_play_id_by_first_2)).alias("first_duration_sec_change"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "duration_sec_cume_dist",
        "duration_sec_percent_rank",
        "first_duration_sec",
        "play_id",
        "play_source",
        "play_device",
        "play_listener_id",
        "min_duration_sec_percent_rank_so_far",
        "play_is_skipped_min_duration_sec",
        "duration_sec_share_of_play_is_skipped",
        "first_duration_sec_change",
        F.round(F.col("duration_sec_cume_dist") * 1.1, 2).alias("duration_sec_cume_dist_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("play_device")), " ", "_")).alias("play_device_slug"),
    )
)
