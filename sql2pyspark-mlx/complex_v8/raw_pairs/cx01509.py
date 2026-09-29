from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_track_id_by_prior_ms_pl = Window.partitionBy("play_track_id").orderBy("prior_ms_played_7d", "id", "play_id")
w_play_count_play_source_by_fi = Window.partitionBy("play_count", "play_source").orderBy(F.col("first_played_at").desc(), "id", "play_id")
w_play_id_play_source = Window.partitionBy("play_id", "play_source")
w_play_count_play_source_by_fi_2 = Window.partitionBy("play_count", "play_source").orderBy(F.col("first_played_at").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

detail_rows = (
    in_window.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_ms_played_7d",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.row_number().over(w_play_track_id_by_prior_ms_pl).alias("seq_no"),
        F.row_number().over(w_play_count_play_source_by_fi).alias("seq_no_2"),
        F.count(F.lit(1)).over(w_play_id_play_source).alias("rows_in_play_id_play_source"),
        F.min("play_count").over(w_play_count_play_source_by_fi_2).alias("min_play_count_so_far"),
    )
)

result = (
    sequenced
    .rollup("listener_id", "play_count", "play_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("seq_no_2"), 2).alias("stddev_seq_no_2"),
        F.sum(F.when(F.col("play_source").isin("search", "radio", "playlist"), 1).otherwise(0)).alias("n_play_source_in_search_radio"),
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
    )
)
