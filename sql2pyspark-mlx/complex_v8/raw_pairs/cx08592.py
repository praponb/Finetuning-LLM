from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_device_release_date_qua = Window.partitionBy("play_device", "release_date_quarter")
w_all_by_play_played_at_runnin = Window.orderBy("play_played_at", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_play_played_at = Window.orderBy("play_played_at", "id", "play_id")
w_play_played_at_decile_artist = Window.partitionBy("play_played_at_decile", "artist_id").orderBy("play_played_at", "id", "play_id")
w_play_played_at_decile = Window.partitionBy("play_played_at_decile")
w_play_played_at_decile_artist_2 = Window.partitionBy("play_played_at_decile", "artist_id").orderBy("play_played_at")
w_artist_id_play_source_by_rel = Window.partitionBy("artist_id", "play_source").orderBy("release_date_quarter", "id", "play_id").rowsBetween(-1, -1)

valid = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

filtered = (
    valid.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") <= 312), "left_semi")
)

with_children = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.hour(F.col("p.played_at")).between(8, 12)), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.release_date_quarter",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

ranked = (
    with_children
    .select(
        "id",
        "artist_id",
        "release_date_quarter",
        "play_id",
        "play_played_at",
        "play_source",
        F.round(F.col("release_date_quarter") / F.nullif(F.sum("release_date_quarter").over(w_play_device_release_date_qua), F.lit(0)), 4).alias("release_date_quarter_share_of_play_device_releas"),
        F.count(F.lit(1)).over(w_all_by_play_played_at_runnin).alias("running_count"),
        F.ntile(10).over(w_all_by_play_played_at).alias("play_played_at_decile"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "play_id",
        "play_played_at",
        "running_count",
        "play_played_at_decile",
        F.lead("play_played_at_decile").over(w_play_played_at_decile_artist).alias("next_play_played_at_decile"),
        F.min("play_played_at_decile").over(w_play_played_at_decile).alias("play_played_at_decile_min_play_played_at_decile"),
        F.lag("play_played_at").over(w_play_played_at_decile_artist).alias("prev_play_played_at"),
        F.rank().over(w_play_played_at_decile_artist_2).alias("play_played_at_rank"),
        F.size(F.collect_set("play_source").over(w_play_played_at_decile)).alias("distinct_play_source_in_play_played_at_decile"),
        F.round(F.avg("play_played_at_decile").over(w_artist_id_play_source_by_rel), 2).alias("play_played_at_decile_avg_1_prior"),
    )
)

result = (
    with_windows
    .filter(F.col("running_count") > 5)
    .select(
        "id",
        "running_count",
        "play_played_at_decile",
        "next_play_played_at_decile",
        "play_played_at_decile_min_play_played_at_decile",
        "prev_play_played_at",
        "distinct_play_source_in_play_played_at_decile",
        "play_played_at_decile_avg_1_prior",
    )
    .orderBy(
        F.col("play_played_at_decile_avg_1_prior").asc(),
        F.col("prev_play_played_at").desc_nulls_first(),
    )
)
