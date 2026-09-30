from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_track_id = Window.partitionBy("played_at_year", "track_id")
w_all_by_max_play_ms_played = Window.orderBy("max_play_ms_played", "id")
w_all_by_max_play_ms_played_fu = Window.orderBy("max_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_max_play_ms_played_ru = Window.orderBy("max_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter((F.col("track_id") <= 9) & (F.col("device") == "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

summarized_rows = (
    scoped_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.total_play_ms_played")
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "played_at_label",
        "source_group",
        "total_play_ms_played",
        F.round(F.col("max_play_ms_played") / F.nullif(F.sum("max_play_ms_played").over(w_played_at_year_track_id), F.lit(0)), 4).alias("max_play_ms_played_share_of_played_at_year_track"),
        (F.col("total_play_ms_played") - F.lag("total_play_ms_played").over(w_all_by_max_play_ms_played)).alias("total_play_ms_played_change"),
        F.nth_value("ms_played", 2).over(w_all_by_max_play_ms_played_fu).alias("second_ms_played"),
        F.count(F.lit(1)).over(w_played_at_year_track_id).alias("rows_in_played_at_year_track_id"),
        F.row_number().over(w_all_by_max_play_ms_played).alias("max_play_ms_played_seq"),
        F.first("played_at_year", ignorenulls=True).over(w_all_by_max_play_ms_played_ru).alias("first_known_played_at_year"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["source", "source_group", "max_play_ms_played_seq"], ["source"], ["source_group"]],
        "source", "source_group", "max_play_ms_played_seq",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("total_play_ms_played_change").alias("total_total_play_ms_played_change"),
        F.max("max_play_ms_played_seq").alias("max_max_play_ms_played_seq"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        F.coalesce(F.col("source_group"), F.lit("ALL")).alias("source_group"),
        "max_play_ms_played_seq",
        "gid",
        "total_total_play_ms_played_change",
        "max_max_play_ms_played_seq",
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("source_group").asc_nulls_last(),
        F.col("max_play_ms_played_seq").asc_nulls_last(),
    )
)
