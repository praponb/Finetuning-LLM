from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_track_duratio = Window.partitionBy("listener_id").orderBy("track_duration_sec", "id")
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_track_artist_id_listener_id = Window.partitionBy("track_artist_id", "listener_id").orderBy(F.col("track_duration_sec").desc(), "id")
w_track_is_explicit_track_arti = Window.partitionBy("track_is_explicit", "track_artist_id").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_track_duratio_2 = Window.partitionBy("listener_id").orderBy("track_duration_sec")
w_track_artist_id_by_track_dur = Window.partitionBy("track_artist_id").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(-6, -1)
w_track_artist_id = Window.partitionBy("track_artist_id")
w_track_is_explicit_by_track_d = Window.partitionBy("track_is_explicit").orderBy("track_duration_sec")

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") <= 214343) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

enriched = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.duration_sec") > 144)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device_clean",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "listener_id",
        "track_id",
        "track_artist_id",
        F.round(F.cume_dist().over(w_listener_id_by_track_duratio), 3).alias("track_duration_sec_cume_dist"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_track_is_explicit), F.lit(0)), 4).alias("track_duration_sec_share_of_track_is_explicit"),
        F.round(F.cume_dist().over(w_track_artist_id_listener_id), 3).alias("track_duration_sec_cume_dist_2"),
        F.last("track_duration_sec", ignorenulls=True).over(w_track_is_explicit_track_arti).alias("last_known_track_duration_sec"),
        F.round(F.avg("track_duration_sec").over(w_track_is_explicit), 2).alias("track_is_explicit_avg_track_duration_sec"),
        F.rank().over(w_listener_id_by_track_duratio_2).alias("track_duration_sec_rank"),
        F.max("track_duration_sec").over(w_track_artist_id_by_track_dur).alias("track_duration_sec_max_6_prior"),
        F.count(F.lit(1)).over(w_track_artist_id).alias("rows_in_track_artist_id"),
        F.dense_rank().over(w_track_is_explicit_by_track_d).alias("track_duration_sec_dense_rank"),
        F.lag("track_duration_sec", 1, 0).over(w_track_artist_id_listener_id).alias("prev_track_duration_sec"),
    )
)

scored = (
    metrics
    .filter(F.col("track_is_explicit_avg_track_duration_sec") <= 321.0)
    .select(
        "*",
        (F.floor(F.col("track_duration_sec_rank") / 2) * 2).alias("track_duration_sec_rank_bucket"),
        F.round(F.col("track_duration_sec_rank") / F.nullif(F.col("rows_in_track_artist_id"), F.lit(0)), 3).alias("track_duration_sec_rank_per_rows_in_track_artist"),
        F.coalesce(F.col("rows_in_track_artist_id"), F.lit(0)).alias("rows_in_track_artist_id_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "track_id",
        "track_artist_id",
        "track_duration_sec_cume_dist",
        "track_duration_sec_cume_dist_2",
        "last_known_track_duration_sec",
        "track_is_explicit_avg_track_duration_sec",
        "track_duration_sec_rank",
        "track_duration_sec_max_6_prior",
        "rows_in_track_artist_id",
        "prev_track_duration_sec",
        "track_duration_sec_rank_per_rows_in_track_artist",
        "rows_in_track_artist_id_filled",
        (F.col("rows_in_track_artist_id") + F.col("prev_track_duration_sec")).alias("rows_in_track_artist_id_plus_prev_track_duration"),
        F.coalesce(F.col("prev_track_duration_sec"), F.lit(0)).alias("prev_track_duration_sec_filled"),
    )
    .orderBy(F.col("track_duration_sec_max_6_prior").desc())
)
