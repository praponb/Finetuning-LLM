from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(-3, Window.currentRow)
w_source = Window.partitionBy("source")
w_source_device_by_ms_played_m = Window.partitionBy("source", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_skipped_flag_n_not_is_ski = Window.partitionBy("is_skipped_flag", "n_not_is_skipped_filled")
w_track_id_is_skipped_flag_by = Window.partitionBy("track_id", "is_skipped_flag").orderBy("n_not_is_skipped", "track_id", "is_skipped")

scoped = (
    df_plays
    .filter((F.col("source").isin("playlist", "radio", "album")) | (F.col("ms_played") > 72464))
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
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id_by_played_at_moving), 2).alias("ms_played_avg_4"),
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
        F.size(F.collect_set("device").over(w_source)).alias("distinct_device_in_source"),
        F.sum("ms_played").over(w_source_device_by_ms_played_m).alias("ms_played_sum_4"),
    )
)

per_group = (
    metrics
    .groupBy("track_id", "is_skipped")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_not_is_skipped"),
        F.count(F.when(F.col("source").isin("playlist", "radio", "search"), 1)).alias("n_source_in_playlist_radio_sea"),
        F.round(F.avg("distinct_device_in_source"), 2).alias("avg_distinct_device_in_source"),
    )
)

screened = (
    per_group.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") > 160264), "left_semi")
)

screened_2 = (
    screened.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.size(F.col("t.moods")) >= 2) & (F.col("t.duration_sec").between(144, 357)), "left_anti")
)

enriched = (
    screened_2
    .select(
        "track_id",
        "is_skipped",
        "n_not_is_skipped",
        F.coalesce(F.col("n_not_is_skipped"), F.lit(0)).alias("n_not_is_skipped_filled"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

ranked = (
    enriched
    .select(
        "track_id",
        "is_skipped",
        "n_not_is_skipped",
        "n_not_is_skipped_filled",
        "is_skipped_flag",
        F.count(F.lit(1)).over(w_is_skipped_flag_n_not_is_ski).alias("rows_in_is_skipped_flag_n_not_is_skipped_filled"),
        F.lead("is_skipped_flag", 1, 0).over(w_track_id_is_skipped_flag_by).alias("next_is_skipped_flag"),
        F.round(F.col("is_skipped_flag") / F.nullif(F.sum("is_skipped_flag").over(w_is_skipped_flag_n_not_is_ski), F.lit(0)), 4).alias("is_skipped_flag_share_of_is_skipped_flag_n_not_i"),
    )
)

result = (
    ranked
    .select(
        "track_id",
        "is_skipped",
        "n_not_is_skipped",
        "is_skipped_flag",
        "rows_in_is_skipped_flag_n_not_is_skipped_filled",
        "next_is_skipped_flag",
        "is_skipped_flag_share_of_is_skipped_flag_n_not_i",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag_2"),
        F.abs(F.col("n_not_is_skipped_filled") - 5).alias("n_not_is_skipped_filled_distance"),
    )
    .orderBy(
        F.col("n_not_is_skipped").desc_nulls_first(),
        F.col("n_not_is_skipped_filled_distance").desc(),
    )
)
