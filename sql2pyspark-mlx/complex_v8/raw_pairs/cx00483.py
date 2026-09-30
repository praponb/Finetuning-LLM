from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_artist_id_is_skipped = Window.partitionBy("track_artist_id", "is_skipped")
w_device_track_artist_id_by_tr = Window.partitionBy("device", "track_artist_id").orderBy(F.col("track_bpm").desc(), "id")
w_listener_id_device_by_track = Window.partitionBy("listener_id", "device").orderBy(F.col("track_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_track_artist_id_by_tr_2 = Window.partitionBy("device", "track_artist_id").orderBy(F.col("track_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_device_by_track_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("track_bpm").desc_nulls_first())
w_all_by_track_bpm_running = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_device_track_artist_id_by_tr_3 = Window.partitionBy("device", "track_artist_id").orderBy(F.col("track_bpm").desc(), "id").rowsBetween(-2, -1)
w_listener_id_device_by_track_3 = Window.partitionBy("listener_id", "device").orderBy(F.col("track_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("ms_played") < 324058))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

joined = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.days_since_played_at",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.max("days_since_played_at").over(w_track_artist_id_is_skipped).alias("track_artist_id_is_skipped_max_days_since_played"),
        F.lag("track_bpm", 3).over(w_device_track_artist_id_by_tr).alias("prev3_track_bpm"),
        F.first("track_bpm", ignorenulls=True).over(w_listener_id_device_by_track).alias("first_known_track_bpm"),
        F.last("track_bpm", ignorenulls=True).over(w_device_track_artist_id_by_tr_2).alias("last_known_track_bpm"),
        F.rank().over(w_listener_id_device_by_track_2).alias("track_bpm_rank"),
        F.min("days_since_played_at").over(w_all_by_track_bpm_running).alias("min_days_since_played_at_so_far"),
        (F.col("track_bpm") - F.lag("track_bpm").over(w_all_by_track_bpm)).alias("track_bpm_change"),
        F.size(F.collect_set("device").over(w_track_artist_id_is_skipped)).alias("distinct_device_in_track_artist_id_is_skipped"),
        F.count(F.lit(1)).over(w_track_artist_id_is_skipped).alias("rows_in_track_artist_id_is_skipped"),
        F.sum("track_bpm").over(w_device_track_artist_id_by_tr_3).alias("track_bpm_sum_2_prior"),
        F.nth_value("track_bpm", 2).over(w_listener_id_device_by_track_3).alias("second_track_bpm"),
        F.size(F.collect_set("track_id").over(w_track_artist_id_is_skipped)).alias("distinct_track_id_in_track_artist_id_is_skipped"),
        F.sum("days_since_played_at").over(w_track_artist_id_is_skipped).alias("track_artist_id_is_skipped_sum_days_since_played"),
    )
)

tagged = ranked.withColumn("device_or_unknown", F.coalesce(F.col("device"), F.lit("UNKNOWN")))

result = (
    tagged
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "played_at_label",
        "track_bpm",
        "track_artist_id_is_skipped_max_days_since_played",
        "prev3_track_bpm",
        "first_known_track_bpm",
        "last_known_track_bpm",
        "track_bpm_rank",
        "track_bpm_change",
        "distinct_device_in_track_artist_id_is_skipped",
        "rows_in_track_artist_id_is_skipped",
        "track_bpm_sum_2_prior",
        "second_track_bpm",
        "distinct_track_id_in_track_artist_id_is_skipped",
        "track_artist_id_is_skipped_sum_days_since_played",
        "device_or_unknown",
        F.round(F.col("days_since_played_at") / F.nullif(F.col("track_bpm_sum_2_prior"), F.lit(0)), 3).alias("days_since_played_at_per_track_bpm_sum_2_prior"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("track_id").asc())
)
