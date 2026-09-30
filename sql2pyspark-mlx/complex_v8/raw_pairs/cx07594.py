from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")

base_plays = (
    df_plays
    .filter((~F.col("device").isin("tv", "desktop")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_details_track = (
    base_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.artist_id") <= 13)
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.sum("duration_sec").alias("total_track_duration_sec"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

profiled = (
    with_details_track.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.track_artist_id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        "t.total_track_duration_sec",
        "t.last_track_release_date",
    )
)

with_trends = (
    profiled
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "device_source_label",
        "played_at_year",
        "track_artist_id",
        "first_track_release_date",
        "last_track_release_date",
        F.lag("total_track_duration_sec", 1, 0).over(w_all_by_played_at).alias("prev_total_track_duration_sec"),
        F.sum("total_track_duration_sec").over(w_track_id_is_skipped).alias("track_id_is_skipped_sum_total_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id_is_skipped).alias("rows_in_track_id_is_skipped"),
        F.round(F.avg("total_track_duration_sec").over(w_track_id_is_skipped), 2).alias("track_id_is_skipped_avg_total_track_duration_sec"),
        F.round(F.col("total_track_duration_sec") / F.nullif(F.sum("total_track_duration_sec").over(w_track_id_is_skipped), F.lit(0)), 4).alias("total_track_duration_sec_share_of_track_id_is_sk"),
        F.size(F.collect_set("track_artist_id").over(w_track_id_is_skipped)).alias("distinct_track_artist_id_in_track_id_is_skipped"),
    )
)

screened = (
    with_trends.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (~F.array_contains(F.col("l.interests"), "jazz")) & (F.upper(F.trim(F.col("l.username"))).like("F%")), "left_semi")
)

listener_id_stats = (
    screened
    .groupBy("listener_id")
    .agg(F.avg("prev_total_track_duration_sec").alias("avg_prev_total_track_duration_sec"))
)

outliers = (
    screened.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.prev_total_track_duration_sec") > F.col("g.avg_prev_total_track_duration_sec"))
    .select(
        "s.*",
        "g.avg_prev_total_track_duration_sec",
        F.round(F.col("s.prev_total_track_duration_sec") - F.col("g.avg_prev_total_track_duration_sec"), 2).alias("prev_total_track_duration_sec_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "listener_id",
        "played_at",
        "device_source_label",
        "track_artist_id",
        "first_track_release_date",
        "track_id_is_skipped_avg_total_track_duration_sec",
        "distinct_track_artist_id_in_track_id_is_skipped",
        "avg_prev_total_track_duration_sec",
        "prev_total_track_duration_sec_minus_avg",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.date_trunc("day", F.col("played_at_year")).alias("played_at_year_day"),
    )
    .orderBy(
        F.col("track_artist_id").asc_nulls_last(),
        F.col("device_source_label").desc_nulls_first(),
    )
)
