from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month = Window.partitionBy(F.date_trunc("month", "played_at"))
w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_month_by = Window.partitionBy("track_id", F.month("played_at")).orderBy("played_at", "id")
w_device_track_id = Window.partitionBy("device", "track_id")
w_played_at_hour_month_is_skip = Window.partitionBy(F.date_trunc("month", "played_at_hour"), "is_skipped").orderBy("played_at")
w_device_is_skipped_by_ms_play_2 = Window.partitionBy("device", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_month_by_2 = Window.partitionBy("track_id", F.month("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_cume_dist = Window.orderBy(F.col("played_at_cume_dist").desc(), "id")
w_all_by_played_at_month_avg_t = Window.orderBy("played_at_month_avg_track_bpm", "id")
w_device_by_first_known_played = Window.partitionBy("device").orderBy(F.col("first_known_played_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_cume_dist_t = Window.orderBy(F.col("played_at_cume_dist").desc())
w_all_by_played_at_cume_dist_f = Window.orderBy(F.col("played_at_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country = Window.partitionBy("artist_country")
w_next2_played_at_hour_year_by = Window.partitionBy(F.year("next2_played_at_hour")).orderBy("first_track_bpm", "id")

recent_plays = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

with_parent_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.size(F.col("t.moods")) >= 1), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    with_parent_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "played_at_hour",
        "track_moods",
        "artist_country",
        F.count(F.lit(1)).over(w_played_at_month).alias("rows_in_played_at_month"),
        F.first("played_at_hour", ignorenulls=True).over(w_device_is_skipped_by_ms_play).alias("first_known_played_at_hour"),
        F.round(F.avg("track_bpm").over(w_played_at_month), 2).alias("played_at_month_avg_track_bpm"),
        F.first("track_bpm").over(w_all_by_played_at_full).alias("first_track_bpm"),
        F.lead("played_at_hour", 2).over(w_track_id_played_at_month_by).alias("next2_played_at_hour"),
        F.count(F.lit(1)).over(w_device_track_id).alias("rows_in_device_track_id"),
        F.round(F.cume_dist().over(w_played_at_hour_month_is_skip), 3).alias("played_at_cume_dist"),
        F.last("played_at_hour").over(w_device_is_skipped_by_ms_play_2).alias("last_played_at_hour"),
        F.sum("track_duration_sec").over(w_track_id_played_at_month_by_2).alias("running_track_duration_sec"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "source",
        "played_at_hour",
        "track_moods",
        "artist_country",
        "rows_in_played_at_month",
        "first_known_played_at_hour",
        "played_at_month_avg_track_bpm",
        "first_track_bpm",
        "next2_played_at_hour",
        "played_at_cume_dist",
        "running_track_duration_sec",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "played_at_hour",
        "track_moods",
        "rows_in_played_at_month",
        "first_known_played_at_hour",
        "next2_played_at_hour",
        "running_track_duration_sec",
        F.lead("first_track_bpm").over(w_all_by_played_at_cume_dist).alias("next_first_track_bpm"),
        F.lead("next2_played_at_hour", 2).over(w_all_by_played_at_month_avg_t).alias("next2_next2_played_at_hour"),
        F.last("next2_played_at_hour").over(w_device_by_first_known_played).alias("last_next2_played_at_hour"),
        F.round(F.percent_rank().over(w_all_by_played_at_cume_dist_t), 3).alias("played_at_cume_dist_percent_rank"),
        F.first("played_at_cume_dist").over(w_all_by_played_at_cume_dist_f).alias("first_played_at_cume_dist"),
        F.size(F.collect_set("device").over(w_artist_country)).alias("distinct_device_in_artist_country"),
        F.rank().over(w_next2_played_at_hour_year_by).alias("first_track_bpm_rank"),
        F.round(F.col("rows_in_played_at_month") / F.nullif(F.sum("rows_in_played_at_month").over(w_artist_country), F.lit(0)), 4).alias("rows_in_played_at_month_share_of_artist_country"),
    )
)

result = (
    windowed
    .select(
        "id",
        "device",
        "played_at_hour",
        "track_moods",
        "rows_in_played_at_month",
        "first_known_played_at_hour",
        "running_track_duration_sec",
        "next_first_track_bpm",
        "next2_next2_played_at_hour",
        "last_next2_played_at_hour",
        "played_at_cume_dist_percent_rank",
        "distinct_device_in_artist_country",
        "first_track_bpm_rank",
        "rows_in_played_at_month_share_of_artist_country",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)
