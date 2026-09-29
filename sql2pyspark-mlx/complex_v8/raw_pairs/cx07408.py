from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_played_at_year_by = Window.partitionBy("is_skipped", "played_at_year").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_played_at_year_by_2 = Window.partitionBy("is_skipped", "played_at_year").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy("played_at_year")
w_listener_id_source_or_unknow = Window.partitionBy("listener_id", "source_or_unknown")
w_device_group_by_played_at = Window.partitionBy("device_group").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_device_group_by_played_at_fu = Window.partitionBy("device_group").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_moving_1 = Window.orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_played_at_dense_rank = Window.partitionBy("played_at_dense_rank")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("device").isin("car", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.upper(F.trim(F.col("t.title"))).like("N%"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.device_group",
        "s.source_or_unknown",
        F.col("t.title").alias("track_title"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.substring(F.trim(F.col("track_title")), 1, 4).alias("track_title_prefix"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.ntile(5).over(w_is_skipped_played_at_year_by).alias("played_at_quintile"),
        F.rank().over(w_is_skipped_played_at_year_by).alias("played_at_rank"),
        F.last("played_at_year", ignorenulls=True).over(w_is_skipped_played_at_year_by_2).alias("last_known_played_at_year"),
        F.round(F.cume_dist().over(w_track_id_is_skipped_by_playe), 3).alias("played_at_year_cume_dist"),
        F.max("played_at_year").over(w_listener_id_source_or_unknow).alias("listener_id_source_or_unknown_max_played_at_year"),
        F.dense_rank().over(w_is_skipped_played_at_year_by).alias("played_at_dense_rank"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w_device_group_by_played_at).alias("played_at_quartile"),
        F.first("last_known_played_at_year").over(w_device_group_by_played_at_fu).alias("first_last_known_played_at_year"),
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.round(F.avg("played_at_year").over(w_all_by_played_at_moving_1), 2).alias("played_at_year_avg_2"),
        F.max("played_at_dense_rank").over(w_played_at_dense_rank).alias("played_at_dense_rank_max_played_at_dense_rank"),
        F.last("played_at_rank", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at_rank"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    sequenced.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

result = (
    with_stats
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "device_group",
        "source_or_unknown",
        "track_title",
        "played_at_year",
        "track_title_prefix",
        "played_at_quintile",
        "played_at_rank",
        "last_known_played_at_year",
        "played_at_year_cume_dist",
        "listener_id_source_or_unknown_max_played_at_year",
        "played_at_dense_rank",
        "played_at_quartile",
        "played_at_seq",
        "played_at_year_avg_2",
        "played_at_dense_rank_max_played_at_dense_rank",
        "last_known_played_at_rank",
        "play_count",
        "last_played_at",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label_2"),
    )
    .orderBy(F.col("track_id").asc(), "last_played_at")
)
